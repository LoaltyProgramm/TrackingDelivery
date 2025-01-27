package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {

	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt))
	if err != nil {
		return 0, err
	}

	resultID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(resultID), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {

	row := s.db.QueryRow("SELECT * FROM parcel WHERE number = :number", sql.Named("number", number))

	p := Parcel{}

	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return Parcel{}, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {

	rows, err := s.db.Query("SELECT * FROM parcel WHERE client = :client", sql.Named("client", client))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Parcel

	for rows.Next() {
		var p Parcel
		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return nil, err
		}
		res = append(res, p)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {

	_, err := s.db.Exec("UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("status", status),
		sql.Named("number", number))
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {

	query := "UPDATE parcel SET address = :address WHERE number = :number AND status = :status"

	_, err := s.db.Exec(query,
		sql.Named("address", address),
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("Error update: parcel not found or status is not '%s'", ParcelStatusRegistered)
		}

		return fmt.Errorf("Erorr delete: parcel not found or status is not '%s'", ParcelStatusRegistered)
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	query := "DELETE FROM parcel WHERE number = :number AND status = :status"

	_, err := s.db.Exec(query,
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("Erorr delete: parcel not found or status is not '%s'", ParcelStatusRegistered)
		}
		return fmt.Errorf("Erorr delete: parcel not found or status is not '%s'", ParcelStatusRegistered)
	}

	return nil
}
