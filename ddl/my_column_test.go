package ddl

import (
	"context"
	"github.com/pingcap/check"
	"testing"
	"time"
)

func TestMyColumn(t *testing.T) {
	c := new(check.C)
	store := testCreateStore(c, "test_column")
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)

	dbInfo := testSchemaInfo(c, d, "test_column")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	<-time.After(20 * time.Second)
}
