package bga

type BPLogger interface {
	Tracef(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}
