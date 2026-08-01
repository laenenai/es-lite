// Package upcast provides a small registry that implements es.Upcaster by
// dispatching to a per-event-type upcast function. It is the schema-evolution
// companion to the (es.v1.schema_version) option: when an event's stored
// version lags the code, the registered function upgrades it on read.
package upcast

// Func upgrades one event of a known type. fromVersion is the event's stored
// schema version; the function decides how far to upgrade (it may chain
// v1->v2->v3 internally). Returning the event unchanged is valid.
type Func[E any] func(fromVersion uint32, event E) (E, error)

// Registry maps a proto type URL to its upcast Func. The zero value is not
// usable; call NewRegistry.
type Registry[E any] struct {
	fns map[string]Func[E]
}

// NewRegistry creates an empty registry.
func NewRegistry[E any]() *Registry[E] {
	return &Registry[E]{fns: make(map[string]Func[E])}
}

// Register sets the upcast function for a proto type URL (e.g.
// "counter.v1.Incremented"). Returns the registry for chaining.
func (r *Registry[E]) Register(typeURL string, fn Func[E]) *Registry[E] {
	r.fns[typeURL] = fn
	return r
}

// Upcast implements es.Upcaster: it invokes the registered function for the
// type, or returns the event unchanged when none is registered.
func (r *Registry[E]) Upcast(typeURL string, fromVersion uint32, event E) (E, error) {
	if fn, ok := r.fns[typeURL]; ok {
		return fn(fromVersion, event)
	}
	return event, nil
}
