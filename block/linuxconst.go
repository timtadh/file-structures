package file

import "os"
import "syscall"

const OPENFLAG = os.O_RDWR | os.O_CREAT | syscall.O_DIRECT
