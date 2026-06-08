package config

import (
	"bufio"
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

var ErrPopulate = errors.New("failed to populate config")
var ErrLoad = errors.New("failed to load config")
var ErrFlush = errors.New("failed to flush config")

type NoHint struct{}

// nonVolatile is a generic interface to be implemented by
// types that have an on-disk representation.
//
// The interface takes care of creating a new file and
// boilerplate buffered read/write. Live reload is not supported.
//
// Some configs may need dynamic path or resource fetching.
// H will be passed to Default() and Suffix().
// Declare H as NoHint when not needed.
type NonVolatile[T any, H any] = nonVolatile[T, H]

type nonVolatile[T any, H any] interface {
	// Default is called when a file doesn't exist.
	Default(*os.Root, *Global, *H) error
	// Suffix is called to determine where the file should live
	// relative to the prefix.
	Suffix(*H) (string, error)
	// MarshalFunc is called to marshal the type into bytes.
	MarshalFunc() ([]byte, error)
	// UnmarshalFunc is called to unmarshal the type from bytes.
	UnmarshalFunc([]byte) error
}

// Args is a type passed to IO-related functions in this module.
type Args[H any] struct {
	Prefix *os.Root
	Global *Global
	// When file is created, perm comes into effect.
	Perm fs.FileMode
	Hint *H
}

// Flush marshals state and saves it to disk.
// It expects parent folders to already exist.
func Flush[T nonVolatile[T, H], H any](state T, args Args[H], logger *slog.Logger) error {
	relPath, err := T.Suffix(state, args.Hint)
	if err != nil {
		return errors.Join(ErrFlush, err)
	}
	l := logger.With("operation", "flush", "path", relPath)

	pfx := args.Prefix

	// perm shouldn't matter here
	f, err := pfx.OpenFile(relPath, os.O_WRONLY, 0o644)
	if err != nil {
		l.Debug("err open file")
		return errors.Join(ErrFlush, err)
	}
	defer f.Close()

	buf, err := T.MarshalFunc(state)
	if err != nil {
		l.Debug("err MarshalFunc()")
		return errors.Join(ErrFlush, err)
	}

	wtr := bufio.NewWriter(f)
	_, err = wtr.Write(buf)
	if err != nil {
		l.Debug("err write buf")
		return errors.Join(ErrFlush, err)
	}
	err = wtr.Flush()
	if err != nil {
		l.Debug("err flush writer")
		return errors.Join(ErrFlush, err)
	}
	l.Info("updated config file")
	return nil
}

// Load only loads and parses an on-disk copy of state.
// It will fail otherwise.
func Load[T nonVolatile[T, H], H any](state T, args Args[H], logger *slog.Logger) error {
	relPath, err := T.Suffix(state, args.Hint)
	if err != nil {
		return errors.Join(ErrLoad, err)
	}
	l := logger.With("operation", "load", "path", relPath)

	pfx := args.Prefix

	// perm shouldn't matter here
	f, err := pfx.OpenFile(relPath, os.O_RDWR, 0o644)
	if err != nil {
		return errors.Join(ErrLoad, err)
	}
	defer f.Close()

	rdr := bufio.NewReader(f)
	var buf bytes.Buffer
	_, err = buf.ReadFrom(rdr)
	if err != nil {
		l.Debug("err read file")
		return errors.Join(ErrLoad, err)
	}
	if buf.Len() == 0 {
		l.Warn("file len is 0, this doesn't look right. try deleting the file.")
	}

	err = T.UnmarshalFunc(state, buf.Bytes())
	if err != nil {
		l.Debug("err UnmarshalFunc()")
		return errors.Join(ErrLoad, err)
	}
	l.Info("loaded config file")
	return nil
}

// Populate first checks if an on-disk file of state exists.
// If so, it unmarshals and loads into state.
//
// Else, it does the following:
//
// - initialize state
//
// - create any parent folders
//
// - marshals state
//
// - writes state
//
// hint must be passed into the function.
// It may or may not be used depending on state's implementation.
func Populate[T nonVolatile[T, H], H any](state T, args Args[H], logger *slog.Logger) error {
	relPath, err := T.Suffix(state, args.Hint)
	if err != nil {
		return errors.Join(ErrPopulate, err)
	}
	l := logger.With("operation", "populate", "path", relPath)

	pfx := args.Prefix
	gc := args.Global
	hint := args.Hint

	if err := pfx.MkdirAll(filepath.Dir(relPath), 0755); err != nil {
		return errors.Join(ErrPopulate, err)
	}

	f, err := pfx.OpenFile(relPath, os.O_RDWR, args.Perm)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		f, err := pfx.OpenFile(relPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, args.Perm)
		if err != nil {
			l.Debug("err create file")
			return errors.Join(ErrPopulate, err)
		}
		defer f.Close()

		err = T.Default(state, pfx, gc, hint)
		if err != nil {
			l.Debug("err Default()")
			return errors.Join(ErrPopulate, err)
		}

		buf, err := T.MarshalFunc(state)
		if err != nil {
			l.Debug("err MarshalFunc()")
			return errors.Join(ErrPopulate, err)
		}

		wtr := bufio.NewWriter(f)
		_, err = wtr.Write(buf)
		if err != nil {
			l.Debug("err write buf")
			return errors.Join(ErrPopulate, err)
		}
		err = wtr.Flush()
		if err != nil {
			l.Debug("err flush writer")
			return errors.Join(ErrPopulate, err)
		}
		l.Info("wrote new config file")
		return nil
	} else if err == nil {
		// already exists
		defer f.Close()
		rdr := bufio.NewReader(f)
		var buf bytes.Buffer
		_, err = buf.ReadFrom(rdr)
		if err != nil {
			l.Debug("err read file")
			return errors.Join(ErrPopulate, err)
		}
		if buf.Len() == 0 {
			l.Warn("file len is 0, this doesn't look right. try deleting the file.")
		}

		err = T.UnmarshalFunc(state, buf.Bytes())
		if err != nil {
			l.Debug("err UnmarshalFunc()")
			return errors.Join(ErrPopulate, err)
		}
		l.Info("loaded config file")
		return nil
	} else {
		// give up
		return err
	}
}
