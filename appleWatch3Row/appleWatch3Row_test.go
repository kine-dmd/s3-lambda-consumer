package appleWatch3Row

import (
	"reflect"
	"testing"
)

/* Export tags must not be changed as they are required during Athena query phase.
   Check that they have not been accidentally modified. */
func TestTagsHaveNotChanged(t *testing.T) {
	// Get the reflection so we can read the tags
	reflection := reflect.TypeOf(AppleWatch3Row{})

	// Read and compare all tags
	field, _ := reflection.FieldByName("Ts")
	compareTag(t, "name=ts, type=UINT_64", field.Tag)

	field, _ = reflection.FieldByName("Rx")
	compareTag(t, "name=rx, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Ry")
	compareTag(t, "name=ry, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Rz")
	compareTag(t, "name=rz, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Rl")
	compareTag(t, "name=rl, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Pt")
	compareTag(t, "name=pt, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Yw")
	compareTag(t, "name=yw, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Ax")
	compareTag(t, "name=ax, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Ay")
	compareTag(t, "name=ay, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Az")
	compareTag(t, "name=az, type=DOUBLE", field.Tag)

	field, _ = reflection.FieldByName("Hr")
	compareTag(t, "name=hr, type=DOUBLE", field.Tag)
}

func compareTag(t *testing.T, expected string, tag reflect.StructTag) {
	actual := tag.Get("parquet")
	if expected != actual {
		t.Fatalf("Tags do not match. Expected %s. Got %s.", expected, actual)
	}
}
