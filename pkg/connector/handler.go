package connector

// Handler ...
type Handler struct {
	fn func(before, after interface{}) error
}

// NewHandler ...
func NewHandler(fn func(before, after interface{}) error) *Handler {
	return &Handler{
		fn: fn,
	}
}

// Create ...
func (h *Handler) Create(after interface{}) error {
	return h.fn(nil, after)
}

// Update ...
func (h *Handler) Update(before, after interface{}) error {
	return h.fn(before, after)
}

// Delete ...
func (h *Handler) Delete(before interface{}) error {
	return h.fn(before, nil)
}
