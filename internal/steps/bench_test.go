package steps

import (
	"fmt"
	"testing"
	"time"

	"github.com/rkshvish/vortara/pkg/row"
)

func benchRow() row.Row {
	data := make(map[string]interface{}, 12)
	for i := 0; i < 8; i++ {
		data[fmt.Sprintf("col_%d", i)] = fmt.Sprintf("value-%d", i)
	}
	data["revenue"] = int64(50000)
	data["status"] = "won"
	data["email"] = "x@example.com"
	data["updated_at"] = time.Now()
	return row.Row{ID: "r1", PrimaryKey: "id=1", Data: data}
}

func benchProcessor(b *testing.B) *Processor {
	p, err := New([]TransformStep{
		{Filter: "status == 'won' AND revenue > 1000"},
		{Rename: map[string]string{"col_0": "name"}},
		{Add: map[string]string{"synced_at": "{{ now() }}"}},
		{Mask: []string{"email"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	return p
}

func BenchmarkApply_FourSteps(b *testing.B) {
	p := benchProcessor(b)
	r := benchRow()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, ok := p.Apply(r)
		if !ok || out.Data == nil {
			b.Fatal("unexpected")
		}
	}
}

func BenchmarkApply_FilterOnly(b *testing.B) {
	p, _ := New([]TransformStep{{Filter: "revenue > 1000"}})
	r := benchRow()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Apply(r)
	}
}
