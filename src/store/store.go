//
// store.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package store

import (
	"sharedis/config"
	"sharedis/store/tikv"
)

func init() {
}

func OpenRaw(conf *config.Config) (DBRaw, error) {
	db, err := tikv.OpenRaw(conf)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func CloseRaw(db DBRaw) error {
	return db.Close()
}
