package main

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	// randSource источник псевдо случайных чисел.
	// Для повышения уникальности в качестве seed
	// используется текущее время в unix формате (в виде числа)
	randSource = rand.NewSource(time.Now().UnixNano())
	// randRange использует randSource для генерации случайных чисел
	randRange = rand.New(randSource)
)

// getTestParcel возвращает тестовую посылку
func getTestParcel() Parcel {
	return Parcel{
		Client:    1000,
		Status:    ParcelStatusRegistered,
		Address:   "test",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

// TestAddGetDelete проверяет добавление, получение и удаление посылки
func TestAddGetDelete(t *testing.T) {

	db, err := sql.Open("sqlite", "tracker.db") // настройте подключение к БД
	require.NoError(t, err)

	store := NewParcelStore(db)
	parcel := getTestParcel()

	parcel.Number, err = store.Add(parcel)
	require.NoError(t, err)
	p, err := store.Get(parcel.Number)
	require.NoError(t, err)
	require.Equal(t, p.Number, parcel.Number)

	require.NoError(t, err)
	require.Equal(t, parcel, p)

	err = store.Delete(parcel.Number)
	require.NoError(t, err)

	_, err = store.Get(parcel.Number)
	require.Equal(t, sql.ErrNoRows, err)
}

// TestSetAddress проверяет обновление адреса
func TestSetAddress(t *testing.T) {

	db, err := sql.Open("sqlite", "tracker.db") // настройте подключение к БД
	require.NoError(t, err)

	store := NewParcelStore(db)
	parcel := getTestParcel()

	parcel.Number, err = store.Add(parcel)
	require.NoError(t, err)
	p, err := store.Get(parcel.Number)
	require.NoError(t, err)
	assert.Equal(t, p, parcel)

	newAddress := "new test address"
	err = store.SetAddress(parcel.Number, newAddress)
	require.NoError(t, err)

	p, err = store.Get(parcel.Number)
	assert.NoError(t, err)
	assert.Equal(t, newAddress, p.Address)
}

// TestSetStatus проверяет обновление статуса
func TestSetStatus(t *testing.T) {

	db, err := sql.Open("sqlite", "tracker.db") // настройте подключение к БД
	require.NoError(t, err)

	store := NewParcelStore(db)
	parcel := getTestParcel()

	parcel.Number, err = store.Add(parcel)
	require.NoError(t, err)
	p, err := store.Get(parcel.Number)
	require.NoError(t, err)
	require.Equal(t, p.Number, parcel.Number)

	err = store.SetStatus(parcel.Number, ParcelStatusSent)
	require.NoError(t, err)

	p, err = store.Get(parcel.Number)
	assert.NoError(t, err)
	assert.Equal(t, p.Status, ParcelStatusSent)
}

func TestGetByClient(t *testing.T) {

	db, err := sql.Open("sqlite", "tracker.db")
	require.NoError(t, err)

	store := NewParcelStore(db)

	parcels := []Parcel{
		getTestParcel(),
		getTestParcel(),
		getTestParcel(),
	}
	parcelMap := map[int]Parcel{}

	client := randRange.Intn(10_000_000)
	parcels[0].Client = client
	parcels[1].Client = client
	parcels[2].Client = client

	// add
	for i := 0; i < len(parcels); i++ {
		id, err := store.Add(parcels[i])
		require.NoError(t, err)
		require.NotEmpty(t, id)

		parcels[i].Number = id

		parcelMap[id] = parcels[i]
	}

	// get by client
	storedParcels, err := store.GetByClient(client)
	require.NoError(t, err)
	assert.Len(t, storedParcels, 3)

	// check
	for _, parcel := range storedParcels {
		assert.Equal(t, parcelMap[parcel.Number], parcel)
	}
}
