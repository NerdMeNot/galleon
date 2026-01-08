//go:build darwin && arm64 && !dev

package galleon

/*
#cgo CFLAGS: -I${SRCDIR}/lib/darwin_arm64
#cgo LDFLAGS: -L${SRCDIR}/lib/darwin_arm64 -lgalleon
*/
import "C"
