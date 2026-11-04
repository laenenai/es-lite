package projection

// Definition is a projection's static metadata: the stable name that keys its
// marker and durable consumer, and the event type URLs it declares as
// consumed. A generated projection exposes exactly one via XDefinition(),
// built from the same name constant and XConsumes() the handlers use, so a
// registry seeded from definitions cannot drift from the code.
type Definition struct {
	Name     string
	Consumes []string
}

// Catalog is a read-only registry of projection definitions. Implementations
// must be safe for concurrent use and must not let a caller mutate the
// definitions they hold: the returned Consumes slice is the caller's to
// modify, never the catalogue's.
type Catalog interface {
	// Definition returns the definition registered under name.
	Definition(name string) (Definition, bool)
	// Definitions returns every definition, in registration order.
	Definitions() []Definition
}

// catalog is an immutable map-backed Catalog. Construct it once with
// NewCatalog; there is no mutation API.
type catalog struct {
	defs   []Definition
	byName map[string]int
}

// NewCatalog returns an immutable Catalog over defs. The input order is kept
// by Definitions. The first definition for a name wins; a later duplicate is
// ignored, so a seed list cannot silently shadow an earlier registration.
// Both the definitions and their Consumes slices are copied, so mutating the
// arguments after the call has no effect.
func NewCatalog(defs ...Definition) Catalog {
	c := &catalog{
		defs:   make([]Definition, 0, len(defs)),
		byName: make(map[string]int, len(defs)),
	}
	for _, d := range defs {
		if _, dup := c.byName[d.Name]; dup {
			continue
		}
		d = cloneDefinition(d)
		c.byName[d.Name] = len(c.defs)
		c.defs = append(c.defs, d)
	}
	return c
}

// Definition implements Catalog.
func (c *catalog) Definition(name string) (Definition, bool) {
	i, ok := c.byName[name]
	if !ok {
		return Definition{}, false
	}
	return cloneDefinition(c.defs[i]), true
}

// Definitions implements Catalog.
func (c *catalog) Definitions() []Definition {
	out := make([]Definition, len(c.defs))
	for i, d := range c.defs {
		out[i] = cloneDefinition(d)
	}
	return out
}

// cloneDefinition deep-copies the definition so callers can never alias the
// catalogue's backing slice.
func cloneDefinition(d Definition) Definition {
	d.Consumes = append([]string(nil), d.Consumes...)
	return d
}
