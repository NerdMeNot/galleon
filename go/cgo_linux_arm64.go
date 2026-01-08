//go:build linux && arm64 && !dev

package galleon

/*
#cgo CFLAGS: -I${SRCDIR}/lib/linux_arm64
#cgo LDFLAGS: -L${SRCDIR}/lib/linux_arm64 -lgalleon
*/
import "C"
