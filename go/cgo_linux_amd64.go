//go:build linux && amd64 && !dev

package galleon

/*
#cgo CFLAGS: -I${SRCDIR}/lib/linux_amd64
#cgo LDFLAGS: -L${SRCDIR}/lib/linux_amd64 -lgalleon
*/
import "C"
