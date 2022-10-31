package repmeta

import (
  "bytes"
  "context"
	reptext "github.com/radiochild/utils/text"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
  "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
  "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type OutputType int

const (
	OTText OutputType = iota
	OTJSON
	OTMessagePack
)

const MinS3BufSize int64 = 5 * 1024 * 1024

type ReportWriter struct {
	logger          *zap.SugaredLogger
	outwriter       io.Writer
	outputType      OutputType
  outputName      string
	spec            *ReportSpec
	levels          []*ReportLevel
	grandTotals     *ReportLevel
	suppressDetails bool
	wantDashes      bool
  s3Client        *s3.Client
  bucketName      string
  uploadId        string
  parts           []types.CompletedPart
  streamBuf       *bytes.Buffer
}

type ReportRow struct {
	RowType    string   `json:"typ" msgpack:"typ"`
	RowLevel   int      `json:"lvl" msgpack:"lvl"`
	LevelName  string   `json:"nam" msgpack:"nam"`
	LevelCount int64    `json:"qty" msgpack:"qty"`
	Values     []string `json:"val" msgpack:"val"`
}

func (rW *ReportWriter) EmitRow(rowType string, rowLevel int, levelName string, levelCount int64, values []string) error {
	rOut := ReportRow{
		RowType:    rowType,
		RowLevel:   rowLevel,
		LevelName:  levelName,
		LevelCount: levelCount,
		Values:     values,
	}
  var oStr string
  var err error
  var oData []byte

	switch rW.outputType {
	case OTText:
		summaryName := rOut.LevelName
		if rOut.LevelCount > 0 {
			summaryName = fmt.Sprintf("%s [%d]", rOut.LevelName, rOut.LevelCount)
		}
		oStr = fmt.Sprintf("%s-%d\t%s\t%s\t\n", rOut.RowType, rOut.RowLevel, summaryName, reptext.TabString(rOut.Values))
    oData = []byte(oStr)
	case OTJSON:
		oData, err = json.Marshal(rOut)
		if err != nil {
			return err
		}
    oData = append(oData, '\n')
	case OTMessagePack:
		oData, err = msgpack.Marshal(rOut)
		if err != nil {
			return err
		}
	}

  // Before emitting, see if this should be buffered/streamed to s3
  useS3 := len(rW.bucketName) > 0
  if useS3 {

    // Append oData buffer to  streaming buffer
    rW.streamBuf.Write(oData)
    bufSize := int64(rW.streamBuf.Len())

    if bufSize > MinS3BufSize {
      // Stream to s3
      rW.Flush(MinS3BufSize)
      rW.streamBuf.Reset()
    }

    return nil
  }

  rW.outwriter.Write(oData)
	return nil
}

func (rW *ReportWriter) FlushRows() error {
	if rW.outputType == OTText {
		tW, ok := rW.outwriter.(*tabwriter.Writer)
		if ok {
			tW.Flush()
		}
	}
	return nil
}

func (rW *ReportWriter) ProcessGrandTotals() {
	grandIndex := 0
	sums := rW.grandTotals.AllTotals()
	dashes := reptext.AllToChar(sums, '-')
	ddashes := reptext.AllToChar(sums, '=')
	summaryText := "Grand Totals"

	if rW.wantDashes {
		rW.EmitRow("TOT", grandIndex, "", 0, dashes)
	}
	rW.EmitRow("TOT", grandIndex, summaryText, rW.grandTotals.TotCount, sums)
	if rW.wantDashes {
		rW.EmitRow("TOT", grandIndex, "", 0, ddashes)
	}
	rW.FlushRows()
}

func NewReportWriter(pLogger *zap.SugaredLogger, wx io.Writer, outputType OutputType, outputName string, suppressDetails bool, spec *ReportSpec, s3Client *s3.Client, bucketName string) *ReportWriter {
	rW := new(ReportWriter)
	rW.logger = pLogger
	rW.outputType = outputType
	rW.outwriter = wx
  rW.outputName = outputName
  rW.bucketName = bucketName
  rW.s3Client = s3Client
  useS3 := len(rW.bucketName) > 0
  if useS3 {
    rW.streamBuf = new(bytes.Buffer)

    //struct for starting a multipart upload
    startInput := s3.CreateMultipartUploadInput{
        Bucket: aws.String(rW.bucketName),
        Key:    aws.String(rW.outputName),
    }

    //send command to start copy and get the upload id as it is needed later
    ctx := context.TODO()
    uploadId := ""
    createOutput, err := rW.s3Client.CreateMultipartUpload(ctx, &startInput)
    if err != nil {
        rW.logger.Fatal(err.Error())
    }
    if createOutput != nil {
        if createOutput.UploadId != nil {
            uploadId = *createOutput.UploadId
        }
    }
    if uploadId == "" {
        rW.logger.Fatal("No upload id found in start upload request")
    }
    rW.uploadId = uploadId
    rW.parts = make([]types.CompletedPart, 0)
  }


	if rW.outputType == OTText {
		// minwidth, tabwidth, padding, padChar
		rW.outwriter = tabwriter.NewWriter(wx, 23, 26, 0, ' ', tabwriter.AlignRight) // |tabwriter.Debug)
		rW.wantDashes = true
	}

	// spec, levels
	var allLevels []*ReportLevel
	rW.spec = spec
	topLevel, erx := NewReportLevel(spec, "")
	if erx != nil {
		rW.logger.Fatalf("Unable to allocate Top Level\n%s\n", erx.Error())
	}
	allLevels = append(allLevels, topLevel)

	for _, grpName := range spec.Groups {
		pLevel, err := NewReportLevel(spec, grpName)
		if err == nil {
			allLevels = append(allLevels, pLevel)
		}
	}
	// var erx error
	// rW.grandTotals, erx = NewReportLevel(spec, "")
	// if erx != nil {
	// 	rW.logger.Fatalf("Unable to allocate Grand Totals\n%s\n", erx.Error())
	// }
	rW.grandTotals = topLevel

	rW.levels = allLevels
	rW.suppressDetails = suppressDetails
	return rW
}

// If dR is nil, this is the final summary, and should return
// the very first level
func (rW *ReportWriter) FindFirstChangedLevel(hasLevels bool, dR *DataRow) int {
	if !hasLevels {
		rW.logger.Fatalf("FindFirstChangedLevel: No levels to analyze!")
	}
	if dR == nil {
		return 0
	}
	// Check if the CurrValue has changed for each Level
	currValue := ""
	idx := -1
	for levelIdx, pLvl := range rW.levels {
		idx = pLvl.FldIdx
		if idx >= 0 {
			currValue = dR.ValueAtIndex(idx)
			if pLvl.PrevValue != currValue {
				return levelIdx
			}
		}
	}
	return -1
}

func (rW *ReportWriter) ProcessHeaders(startLevel int, lastLevel int, dR *DataRow) int {
	numProcessed := 0

	// ------------------------------------------------------------
	// Not yet sure what this is used for
	// Maybe if there were no groups???
	// ------------------------------------------------------------
	if startLevel < 0 && lastLevel == 0 {
		titles := rW.ColumnDisplayNames()
		dashes := reptext.AllToChar(titles, '-')
		rW.EmitRow("HDR", lastLevel, "", 0, titles)
		if rW.wantDashes {
			rW.EmitRow("HDR", lastLevel, "", 0, dashes)
		}
		return 1
	}

	if startLevel < 0 {
		rW.logger.Infof("ProcessHeaders(%d, %d, dR)", startLevel, lastLevel)
		return 0
	}

	// When details are being suppressed, we suppress the headers and only output the footers
	for levelIndex := startLevel; levelIndex <= lastLevel; levelIndex++ {
		workLevel := rW.levels[levelIndex]
		currValue := dR.ValueAtIndex(workLevel.FldIdx)
		workLevel.PrevValue = currValue
		if !rW.suppressDetails {
			rW.EmitRow("HDR", levelIndex, currValue, 0, []string{})

			if levelIndex == lastLevel {
				titles := rW.ColumnDisplayNames()
				dashes := reptext.AllToChar(titles, '-')
				rW.EmitRow("HDR", lastLevel, "", 0, titles)
				if rW.wantDashes {
					rW.EmitRow("HDR", lastLevel, "", 0, dashes)
				}
			}
		}
		numProcessed++
	}
	return numProcessed
}

func (rW *ReportWriter) ColumnDisplayNames() []string {
	var dspNames []string
	dspName := ""
	colNames := ColSpecFldNames(rW.spec.Columns)
	allCols := append(rW.spec.ExtraColumns, colNames...)
	for _, colName := range allCols {
		colIdx, fldSpec := rW.spec.ColumnNamed(colName)
		dspName = colName
		if colIdx >= 0 {
			dspName = fldSpec.ColName
		}
		dspNames = append(dspNames, dspName)
	}
	return dspNames
}

func (rW *ReportWriter) ProcessFooters(startLevel int, lastLevel int) int {
	numProcessed := 0

	for levelIndex := lastLevel; levelIndex >= startLevel; levelIndex-- {
		workLevel := rW.levels[levelIndex]
		sums := workLevel.AllTotals()
		dashes := reptext.AllToChar(sums, '-')
		ddashes := reptext.AllToChar(sums, '=')
		summaryText := fmt.Sprintf("%s", workLevel.PrevValue)

		if rW.wantDashes {
			rW.EmitRow("SUM", levelIndex, "", 0, dashes)
		}
		rW.EmitRow("SUM", levelIndex, summaryText, workLevel.TotCount, sums)
		if rW.wantDashes {
			rW.EmitRow("SUM", levelIndex, "", 0, ddashes)
			rW.EmitRow("SUM", levelIndex, "", 0, []string{})
		}

		workLevel.ResetNumerics()
		workLevel.TotCount = 0
		numProcessed++
	}
	return numProcessed
}

func DetailWriter(ctx interface{}, dR *DataRow) {
	rW := ctx.(*ReportWriter)
	if rW != nil {
		rW.HandleDataRow(dR)
		return
	}
	panic("No ReportWriter available")
}

func (rW *ReportWriter) HandleDataRow(dR *DataRow) {
	lastLevel := len(rW.levels)
	hasLevels := lastLevel > 0
	if hasLevels {
		lastLevel--
	}

	hasRec := dR != nil
	rowsCounted := rW.grandTotals.TotCount
	altRowsCounted := rW.levels[0].TotCount
	if altRowsCounted != rowsCounted {
		rW.logger.Infof("grandTotalCount=%d  level0Count=%d", rowsCounted, altRowsCounted)
	}
	isFirst := rowsCounted == 0

	footerCount := 0
	changedLevel := -1
	if !isFirst && hasLevels {
		changedLevel = rW.FindFirstChangedLevel(hasLevels, dR)
		if changedLevel != -1 {
			footerCount = rW.ProcessFooters(changedLevel, lastLevel)
		}
	}

	hadFooters := footerCount > 0

	if (hadFooters || isFirst) && hasRec {
		rW.ProcessHeaders(changedLevel, lastLevel, dR)
	}

	if hasRec {
		if !rW.suppressDetails {
			rW.EmitRow("DET", lastLevel, "", 0, dR.AllValues())
		}
		for _, lvl := range rW.levels {
			lvl.DidAccumulate(dR)
		}
		rW.grandTotals.DidAccumulate(dR)
	}

	rW.FlushRows()
}

func (rW *ReportWriter) String() string {
	var lines []string

	if rW.suppressDetails {
		line2 := fmt.Sprintf("Suppress Details: %t", rW.suppressDetails)
		lines = append(lines, line2)
	}

	for _, pLevel := range rW.levels {
		lines = append(lines, pLevel.String())
	}
	if len(rW.levels) > 0 {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

func ShowReportWriter(rW *ReportWriter) {
	ctxLines := strings.Split(rW.String(), "\n")
	for _, lx := range ctxLines {
		rW.logger.Infof("%s\n", lx)
	}
}

// minSize 1 is used to flush anything remaining
// Otherwise, minSize is 5 * 1024 * 1024
func (rW *ReportWriter) Flush(minSize int64) error {
  usesS3 := len(rW.bucketName) > 0
  if !usesS3 {
    return nil
  }

  ctx := context.TODO()
  var err error
  var partOutput *s3.UploadPartOutput

  numbytes := int64(rW.streamBuf.Len())
  if numbytes >= minSize {
    partNumber := int32(len(rW.parts) + 1)
    partInput := s3.UploadPartInput{
      Bucket:          aws.String(rW.bucketName),
      Key:             aws.String(rW.outputName),
      PartNumber:      partNumber,
      UploadId:        aws.String(rW.uploadId),
      Body:            rW.streamBuf,
      ContentLength:   numbytes,
    }
    rW.logger.Debugf("Attempting to upload part %d", partNumber)
    partOutput, err = rW.s3Client.UploadPart(ctx, &partInput)

    // If successful, copy the part and retain for the summary
    // Part is an eTag and its related partNumber
    if err == nil {
      part := types.CompletedPart {
        ETag: partOutput.ETag,
        PartNumber: partNumber,
      }
      rW.parts = append(rW.parts, part)
      rW.logger.Infof("Successfully uploaded part %d (ETag: %s) to %s/%s", partNumber, *partOutput.ETag, rW.bucketName, rW.outputName)
    }
  }
  return err 
}

func (rW *ReportWriter) CompleteUpload() error {
  usesS3 := len(rW.bucketName) > 0
  if !usesS3 {
    return nil
  }

  ctx := context.TODO()

  // Complete the MultiPart operation
  //create struct for completing the upload
  mpu := types.CompletedMultipartUpload{
     Parts: rW.parts,
  }

  //complete actual upload
  //does not actually copy if the complete command is not received
  complete := s3.CompleteMultipartUploadInput{
    Bucket:          aws.String(rW.bucketName),
    Key:             aws.String(rW.outputName),
    UploadId:        aws.String(rW.uploadId),
    MultipartUpload: &mpu,
  }
  compOutput, err := rW.s3Client.CompleteMultipartUpload(ctx, &complete)
  if err != nil {
    return err
  }

  if compOutput == nil {
    return fmt.Errorf("Unable to complete multipart upload (but err was nil)")
  }

  rW.logger.Infof("Successfully uploaded to %s/%s", rW.bucketName, rW.outputName)
  return nil
}

func (rW *ReportWriter) Close() error {
  err := rW.Flush(1) // Any buffered data still needs to be sent

  if err != nil {
    return err
  }

  err = rW.CompleteUpload()
  return err
}
