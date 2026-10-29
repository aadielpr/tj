package provider

import (
	"log"

	"github.com/go-playground/validator/v10"
)

type Validator struct {
	Validate *validator.Validate
}

func (v *Validator) Init() {
	validate := validator.New()
	err := validate.RegisterValidation("unixtimestamp", validateUnix)
	if err != nil {
		log.Fatalf("Failed to register validation: %v", err)
	}

	v.Validate = validate
}

func validateUnix(fl validator.FieldLevel) bool {
	val := fl.Field().Int()

	minTs := int64(0)
	maxTs := int64(1798736399)

	return val >= minTs && val <= maxTs
}
