//go:build windows && amd64 && !dev

package galleon

/*
#cgo CFLAGS: -I${SRCDIR}/lib/windows_amd64
#cgo LDFLAGS: -L${SRCDIR}/lib/windows_amd64 -lgalleon
*/
import "C"
