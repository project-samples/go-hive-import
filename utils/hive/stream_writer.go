package hive

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	. "github.com/beltran/gohive"
)

type StreamWriter struct {
	connection   *Connection
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	VersionIndex int
	schema       *Schema
	batchSize    int
	batch        []interface{}
}

func NewStreamWriter(connection *Connection, tableName string, modelType reflect.Type, batchSize int, options ...func(context.Context, interface{}) (interface{}, error)) *StreamWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}

	return NewHiveStreamWriter(connection, tableName, modelType, batchSize, mp, nil)
}

func NewHiveStreamWriter(connection *Connection, tableName string, modelType reflect.Type, batchSize int,
	mp func(context.Context, interface{}) (interface{}, error), options ...func(i int) string) *StreamWriter {
	versionIndex := -1
	schema := CreateSchema(modelType)
	return &StreamWriter{connection: connection, schema: schema, tableName: tableName, batchSize: batchSize, Map: mp, VersionIndex: versionIndex}
}

func (w *StreamWriter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		w.batch = append(w.batch, m2)
	} else {
		w.batch = append(w.batch, model)
	}
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamWriter) Flush(ctx context.Context) error {
	cursor := w.connection.Cursor()
	defer cursor.Close()

	insertQ := BuildToInsertStream(w.tableName, w.batch[0], w.VersionIndex, false, w.schema)
	var arrayValues []string
	for _, v := range w.batch {
		value := BuildToInsertStreamValues(w.tableName, v, w.VersionIndex, false, w.schema)
		arrayValues = append(arrayValues, value)
	}
	streamValues := strings.Join(arrayValues, ",")
	stm := fmt.Sprintf(insertQ + streamValues)

	defer func() {
		w.batch = make([]interface{}, 0)
	}()

	cursor.Exec(ctx, stm)
	if cursor.Err != nil {
		return cursor.Err
	}
	return nil
}
