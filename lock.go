package locker

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"strconv"
	"sync"
)

func buildPrefix(id int64) string {
	return fmt.Sprintf("[Locker:%q]", strconv.FormatInt(id, 10))
}

// Locker is an interface for postgresql advisory locks.
type Locker interface {
	Lock(ctx context.Context) (Releaser, error)
}

type Releaser interface {
	Release(ctx context.Context) error
}

// New returns a Locker client.
func New(db *sql.DB, key string) (Locker, error) {
	id, err := hash(key)
	if err != nil {
		return nil, fmt.Errorf("create hash of key: %w", err)
	}

	return &lockStorage{
		id: int64(id),
		l:  log.New(log.Writer(), buildPrefix(int64(id)), log.LstdFlags),
		db: db,
		mu: &sync.Mutex{},
	}, err
}

type lockStorage struct {
	id int64
	l  *log.Logger
	db *sql.DB
	mu *sync.Mutex
}

func (s *lockStorage) Lock(ctx context.Context) (Releaser, error) {
	// Protect connection pool from running out of connections
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("obtain db connection: %w", err)
	}

	_, err = conn.ExecContext(ctx, "SELECT PG_ADVISORY_LOCK($1)", s.id)
	if err != nil {
		return nil, fmt.Errorf("obtain advisory lock: %w", err)
	}

	s.l.Println("Lock acquired")

	return &lock{
		id:   s.id,
		l:    s.l,
		conn: conn,
	}, nil
}

// Lock implements the Locker interface.
type lock struct {
	id   int64
	l    *log.Logger
	conn *sql.Conn
}

func (l lock) Release(ctx context.Context) error {
	defer l.l.Println("Lock released")

	// Return connection to the connection pool to release lock
	defer func() {
		if err := l.conn.Close(); err != nil {
			l.l.Printf("[ERROR] Failed to return conn to the pool: %v", err)
		}
	}()

	// Release the advisory lock
	_, err := l.conn.ExecContext(ctx, "SELECT PG_ADVISORY_UNLOCK($1)", l.id)

	return err
}

func hash(s string) (uint32, error) {
	h := fnv.New32()

	if _, err := h.Write([]byte(s)); err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}
