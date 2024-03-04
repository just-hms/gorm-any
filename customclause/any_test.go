package customclause_test

import (
	"fmt"
	"log"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/just-hms/gorm-any/customclause"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestPreloadWithAnyUint(t *testing.T) {
	req := require.New(t)

	dsn := url.URL{
		User:     url.UserPassword("kek", "kek"),
		Scheme:   "postgres",
		Host:     fmt.Sprintf("%s:%s", "host", "5432"),
		Path:     "kek",
		RawQuery: (&url.Values{"sslmode": []string{"disable"}}).Encode(),
	}

	db, err := gorm.Open(postgres.Open(dsn.String()), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger: logger.New(log.Default(), logger.Config{
			IgnoreRecordNotFoundError: true,
			LogLevel:                  logger.Error,
		}),
	})
	req.NoError(err)

	tx := db.Begin()
	defer tx.Rollback()

	type Dog struct {
		ID       uint `gorm:"primaryKey"`
		PersonID uint
	}

	type Person struct {
		ID   uint  `gorm:"primaryKey"`
		Dogs []Dog `gorm:"foreignKey:PersonID"`
	}

	err = tx.AutoMigrate(&Person{}, &Dog{})
	req.NoError(err)

	// Create a lot of data
	const (
		peopleCount = 70_000
		dogsCount   = 2
	)

	people := make([]Person, 0, peopleCount)
	for i := range peopleCount {
		people = append(people, Person{
			ID: uint(i) + 1,
		})
	}
	err = tx.CreateInBatches(&people, 1000).Error
	req.NoError(err)

	dogs := make([]Dog, 0, dogsCount)
	for i := range dogsCount {
		dogs = append(dogs, Dog{PersonID: people[i].ID})
	}

	err = tx.CreateInBatches(dogs, 1000).Error
	req.NoError(err)

	people = make([]Person, 0)

	// Preload and retrieve data
	err = tx.Preload("Dogs").Find(&people).Error
	req.Error(err)

	customclause.UseAny(tx)

	err = tx.Preload("Dogs").Find(&people).Error
	req.NoError(err)
	req.Equal(peopleCount, len(people))

	for i, person := range people {
		if i < dogsCount {
			req.Equal(1, len(person.Dogs))
		} else {
			req.Equal(0, len(person.Dogs))
		}
	}

}

func TestPreloadWithAnyUUID(t *testing.T) {
	req := require.New(t)

	dsn := url.URL{
		User:     url.UserPassword("kek", "kek"),
		Scheme:   "postgres",
		Host:     fmt.Sprintf("%s:%s", "host", "5432"),
		Path:     "kek",
		RawQuery: (&url.Values{"sslmode": []string{"disable"}}).Encode(),
	}

	db, err := gorm.Open(postgres.Open(dsn.String()), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger: logger.New(log.Default(), logger.Config{
			IgnoreRecordNotFoundError: true,
			LogLevel:                  logger.Error,
		}),
	})
	req.NoError(err)

	tx := db.Begin()
	defer tx.Rollback()

	type Dog struct {
		ID       uuid.UUID `gorm:"primaryKey;type:uuid"`
		PersonID uuid.UUID `gorm:"type:uuid"`
	}

	type Person struct {
		ID   uuid.UUID `gorm:"primaryKey;type:uuid"`
		Dogs []Dog     `gorm:"foreignKey:PersonID"`
	}

	err = tx.AutoMigrate(&Person{}, &Dog{})
	req.NoError(err)

	// Create a lot of data
	const (
		peopleCount = 70_000
		dogsCount   = 2
	)

	people := make([]Person, 0, peopleCount)
	for range peopleCount {
		people = append(people, Person{
			ID: uuid.New(),
		})
	}
	err = tx.CreateInBatches(&people, 1000).Error
	req.NoError(err)

	dogs := make([]Dog, 0, dogsCount)
	for i := range dogsCount {
		dogs = append(dogs, Dog{
			ID:       uuid.New(),
			PersonID: people[i].ID,
		})
	}

	err = tx.CreateInBatches(dogs, 1000).Error
	req.NoError(err)

	people = make([]Person, 0)

	// Preload and retrieve data
	err = tx.Preload("Dogs").Find(&people).Error
	req.Error(err)

	customclause.UseAny(tx)

	err = tx.Preload("Dogs").Find(&people).Error
	req.NoError(err)
	req.Equal(peopleCount, len(people))

	for i, person := range people {
		if i < dogsCount {
			req.Equal(1, len(person.Dogs))
		} else {
			req.Equal(0, len(person.Dogs))
		}
	}
}

func BenchmarkAny(b *testing.B) {
	req := require.New(b)

	dsn := url.URL{
		User:     url.UserPassword("kek", "kek"),
		Scheme:   "postgres",
		Host:     fmt.Sprintf("%s:%s", "host", "5432"),
		Path:     "kek",
		RawQuery: (&url.Values{"sslmode": []string{"disable"}}).Encode(),
	}

	db, err := gorm.Open(postgres.Open(dsn.String()), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger: logger.New(log.Default(), logger.Config{
			IgnoreRecordNotFoundError: true,
			LogLevel:                  logger.Error,
		}),
	})
	req.NoError(err)

	tx := db.Begin()
	defer tx.Rollback()

	type Dog struct {
		ID       uuid.UUID `gorm:"primaryKey;type:uuid"`
		PersonID uuid.UUID `gorm:"type:uuid"`
	}

	type Person struct {
		ID   uuid.UUID `gorm:"primaryKey;type:uuid"`
		Dogs []Dog     `gorm:"foreignKey:PersonID"`
	}

	err = tx.AutoMigrate(&Person{}, &Dog{})
	req.NoError(err)

	// Create a lot of data
	const (
		peopleCount        = 65_000
		dogsPerPersonCount = 10
	)

	people := make([]Person, 0, peopleCount)
	for range peopleCount {
		people = append(people, Person{
			ID: uuid.New(),
		})
	}
	err = tx.CreateInBatches(&people, 10_000).Error
	req.NoError(err)

	dogs := make([]Dog, 0, dogsPerPersonCount)
	for i := range peopleCount {
		for range dogsPerPersonCount {
			dogs = append(dogs, Dog{
				ID:       uuid.New(),
				PersonID: people[i].ID,
			})
		}
	}
	err = tx.CreateInBatches(dogs, 10_000).Error
	req.NoError(err)

	b.Run("without any", func(b *testing.B) {
		people := make([]Person, 0)

		for range b.N {
			// Preload and retrieve data
			err = tx.Preload("Dogs").Find(&people).Error
			req.NoError(err)
		}
	})

	customclause.UseAny(tx)

	b.Run("with any", func(b *testing.B) {
		people := make([]Person, 0)

		for range b.N {
			// Preload and retrieve data
			err = tx.Preload("Dogs").Find(&people).Error
			req.NoError(err)
		}
	})

	tx.Rollback()
}
