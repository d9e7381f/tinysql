package ddl

import (
	"context"
	"github.com/pingcap/check"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"sync"
	"testing"
)

func TestAddColumn(g *testing.T) {
	c := new(check.C)
	s := new(testColumnSuite)
	s.store = testCreateStore(c, "test_column")
	s.d = newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)

	s.dbInfo = testSchemaInfo(c, s.d, "test_column")
	testCreateSchema(c, testNewContext(s.d), s.d, s.dbInfo)

	d := newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	tblInfo := testTableInfo(c, d, "t", 3)
	ctx := testNewContext(d)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	oldRow := types.MakeDatums(int64(1), int64(2), int64(3))
	handle, err := t.AddRecord(ctx, oldRow)
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	newColName := "c4"
	defaultColValue := int64(4)

	var mu sync.Mutex
	var hookErr error
	checkOK := false

	tc := &TestDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		mu.Lock()
		defer mu.Unlock()
		if checkOK {
			return
		}

		t, err1 := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}
		newCol := table.FindCol(t.(*tables.TableCommon).Columns, newColName)
		if newCol == nil {
			return
		}

		err1 = s.checkAddColumn(newCol.State, d, tblInfo, handle, newCol, oldRow, defaultColValue)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}

		if newCol.State == model.StatePublic {
			checkOK = true
		}
	}

	d.SetHook(tc)

	// Use local ddl for callback test.
	s.d.Stop()

	d.Stop()
	d.start(context.Background(), nil)

	job := testCreateColumn(c, ctx, d, s.dbInfo, tblInfo, newColName, defaultColValue)

	testCheckJobDone(c, d, job, true)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(errors.ErrorStack(hErr), Equals, "")
	c.Assert(ok, IsTrue)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	d.Stop()
	s.d.start(context.Background(), nil)
}

func TestDropColumn(g *testing.T) {
	c := new(check.C)
	s := new(testColumnSuite)
	s.store = testCreateStore(c, "test_column")
	s.d = newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)

	s.dbInfo = testSchemaInfo(c, s.d, "test_column")
	testCreateSchema(c, testNewContext(s.d), s.d, s.dbInfo)

	d := newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	tblInfo := testTableInfo(c, d, "t", 4)
	ctx := testNewContext(d)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	colName := "c4"
	defaultColValue := int64(4)
	row := types.MakeDatums(int64(1), int64(2), int64(3))
	_, err = t.AddRecord(ctx, append(row, types.NewDatum(defaultColValue)))
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	checkOK := false
	var hookErr error
	var mu sync.Mutex

	tc := &TestDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		mu.Lock()
		defer mu.Unlock()
		if checkOK {
			return
		}
		t, err1 := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}
		col := table.FindCol(t.(*tables.TableCommon).Columns, colName)
		if col == nil {
			checkOK = true
			return
		}
	}

	d.SetHook(tc)

	// Use local ddl for callback test.
	s.d.Stop()

	d.Stop()
	d.start(context.Background(), nil)

	job := testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, colName, false)
	testCheckJobDone(c, d, job, false)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(hErr, IsNil)
	c.Assert(ok, IsTrue)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	d.Stop()
	s.d.start(context.Background(), nil)
}
