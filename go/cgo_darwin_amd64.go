//go:build darwin && amd64 && !dev

package galleon

/*
#cgo CFLAGS: -I${SRCDIR}/lib/darwin_amd64
#cgo LDFLAGS: -L${SRCDIR}/lib/darwin_amd64 -lgalleon
*/
import "C"
