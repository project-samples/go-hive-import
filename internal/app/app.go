package app

import (
	"context"
	"path/filepath"
	"time"

	"github.com/beltran/gohive"
	b "github.com/core-go/hive/batch"
	im "github.com/core-go/io/importer"
	rd "github.com/core-go/io/reader"
	v "github.com/core-go/io/validator"
	"github.com/core-go/log"
)

type ApplicationContext struct {
	Import func(ctx context.Context) (int, int, error)
}

func NewApp(ctx context.Context, cfg Config) (*ApplicationContext, error) {
	configuration := gohive.NewConnectConfiguration()
	configuration.PollIntervalInMillis = 3000
	configuration.Database = "masterdata"
	connection, errConn := gohive.Connect(cfg.Hive.Host, cfg.Hive.Port, cfg.Hive.Auth, configuration)
	if errConn != nil {
		return nil, errConn
	}

	fileType := rd.DelimiterType
	filename := ""
	if fileType == rd.DelimiterType {
		filename = "delimiter.csv"
	} else {
		filename = "fixedlength.csv"
	}
	generateFileName := func() string {
		fullPath := filepath.Join("export", filename)
		return fullPath
	}
	reader, err := rd.NewFileReader(generateFileName)
	if err != nil {
		return nil, err
	}
	mp := map[string]interface{}{
		"app": "import users",
		"env": "dev",
	}
	transformer, err := rd.NewDelimiterTransformer[User](",")
	if err != nil {
		return nil, err
	}
	validator, err := v.NewValidator[*User]()
	if err != nil {
		return nil, err
	}
	errorHandler := im.NewErrorHandler[*User, string](log.ErrorFields, "fileName", "lineNo", mp)
	writer := b.NewStreamWriter[*User](connection, "users", 4)
	importer := im.NewImporter(reader.Read, transformer.Transform, validator.Validate, errorHandler.HandleError, errorHandler.HandleException, filename, writer.Write, writer.Flush)
	return &ApplicationContext{Import: importer.Import}, nil
}

type User struct {
	Id          string     `json:"id" gorm:"column:id;primary_key" bson:"_id" format:"%011s" length:"11" dynamodbav:"id" firestore:"id" validate:"required,max=40"`
	Username    string     `json:"username" gorm:"column:username" bson:"username" length:"10" dynamodbav:"username" firestore:"username" validate:"required,username,max=100"`
	Email       string     `json:"email" gorm:"column:email" bson:"email" dynamodbav:"email" firestore:"email" length:"31" validate:"email,max=100"`
	Phone       string     `json:"phone" gorm:"column:phone" bson:"phone" dynamodbav:"phone" firestore:"phone" length:"20" validate:"required,phone,max=18"`
	Status      string     `json:"status" gorm:"column:status" true:"1" false:"0" bson:"status" dynamodbav:"status" format:"%5s" length:"5" firestore:"status"`
	CreatedDate *time.Time `json:"createddate" gorm:"column:createddate" bson:"createddate" length:"10" format:"dateFormat:2006-01-02" dynamodbav:"createddate" firestore:"createddate" validate:"required"`
}
