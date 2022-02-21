//
// tidis.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package sharedis

// wrapper for kv storage engine  operation

import (
	"sharedis/config"
	"sharedis/store"
	"sync"
)

type Sharedis struct {
	conf *config.Config
	dbRaw   store.DBRaw

	wLock sync.RWMutex
	Lock  sync.Mutex
	wg    sync.WaitGroup

}

func NewSharedis(conf *config.Config) (*Sharedis, error) {
	var err error

	sharedis := &Sharedis{
		conf:        conf,
	}
	sharedis.dbRaw, err = store.OpenRaw(conf)
	if err != nil {
		return nil, err
	}

	return sharedis, nil
}

func (sharedis *Sharedis) Close() error {
	err := store.CloseRaw(sharedis.dbRaw)
	if err != nil {
		return err
	}
	return nil
}
