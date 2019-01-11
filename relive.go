package logger

import (
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Txn struct {
	levels   []zapcore.Level
	msgs     []string
	fields   [][]zap.Field
	times    []time.Time
	mu       *sync.Mutex
	aborted  bool
	finished bool
	deadline time.Time
	internal *zap.Logger
}

func newTxn(l *zap.Logger, timeout time.Duration) *Txn {
	return &Txn{
		mu:       new(sync.Mutex),
		levels:   make([]zapcore.Level, 0),
		msgs:     make([]string, 0),
		fields:   make([][]zap.Field, 0),
		times:    make([]time.Time, 0),
		deadline: time.Now().Add(timeout),
		internal: l,
	}
}

func (t *Txn) Finish() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.finished = true
}

func (t *Txn) Abort() {
	t.flush()
}

func (t *Txn) flush() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, v := range t.levels {
		if ce := t.internal.Check(v, t.msgs[i]); ce != nil {
			ce.Time = t.times[i]
			ce.Write(t.fields[i]...)
		}
	}
	t.finished = true
}

type Logger struct {
	internal *zap.Logger
	sequence uint64
	txn      map[uint64]*Txn
	mu       *sync.Mutex
	timeout  time.Duration
	sync     bool
}

func NewLogger(l *zap.Logger, synchronize bool) *Logger {
	logger := &Logger{internal: l, txn: make(map[uint64]*Txn), mu: new(sync.Mutex), timeout: 30 * time.Second, sync: synchronize}
	if !synchronize {
		go logger.backgroundFlush()
	}
	return logger
}

func (l *Logger) Panic(txn *Txn, msg string, field ...zap.Field) {
	l.internal.Panic(msg, field...)
}

func (l *Logger) Fatal(txn *Txn, msg string, field ...zap.Field) {
	l.internal.Fatal(msg, field...)
}

func (l *Logger) Error(txn *Txn, msg string, field ...zap.Field) {
	l.internal.Error(msg, field...)
}

func (l *Logger) Warn(txn *Txn, msg string, field ...zap.Field) {
	l.internal.Warn(msg, field...)
}

func (l *Logger) Info(txn *Txn, msg string, field ...zap.Field) {
	l.internal.Info(msg, field...)
}

func (l *Logger) Debug(txn *Txn, msg string, field ...zap.Field) *Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if l.sync {
		l.internal.Debug(msg, field...)
	}

	if txn == nil {
		txn = l.newTxn()
	}
	if txn.finished {
		return nil
	}

	txn.levels = append(txn.levels, zapcore.DebugLevel)
	txn.msgs = append(txn.msgs, msg)
	txn.fields = append(txn.fields, field)
	txn.times = append(txn.times, time.Now())
	return txn
}

func (l *Logger) newTxn() *Txn {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := atomic.AddUint64(&l.sequence, 1)
	l.txn[n] = newTxn(l.internal, l.timeout)
	return l.txn[n]
}

func (l *Logger) backgroundFlush() {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			l.mu.Lock()
			now := time.Now()
			for k, v := range l.txn {
				if v.finished {
					delete(l.txn, k)
					continue
				}

				if v.deadline.Before(now) {
					v.flush()
					delete(l.txn, k)
					continue
				}
			}
			l.mu.Unlock()
		}
	}
}
