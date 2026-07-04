package poros

import (
	"time"
)

// modifyNumericValue handles numeric additions across all Go numeric primitive types.
func modifyNumericValue[V any](current V, delta int64) (V, int64, error) {
	switch cv := any(current).(type) {
	case int:
		res := cv + int(delta)
		return any(res).(V), int64(res), nil
	case int64:
		res := cv + delta
		return any(res).(V), res, nil
	case int32:
		res := cv + int32(delta)
		return any(res).(V), int64(res), nil
	case int16:
		res := cv + int16(delta)
		return any(res).(V), int64(res), nil
	case int8:
		res := cv + int8(delta)
		return any(res).(V), int64(res), nil
	case uint:
		res := cv + uint(delta)
		return any(res).(V), int64(res), nil
	case uint64:
		res := cv + uint64(delta)
		return any(res).(V), int64(res), nil
	case uint32:
		res := cv + uint32(delta)
		return any(res).(V), int64(res), nil
	case uint16:
		res := cv + uint16(delta)
		return any(res).(V), int64(res), nil
	case uint8:
		res := cv + uint8(delta)
		return any(res).(V), int64(res), nil
	case float64:
		res := cv + float64(delta)
		return any(res).(V), int64(res), nil
	case float32:
		res := cv + float32(delta)
		return any(res).(V), int64(res), nil
	default:
		return current, 0, ErrNotNumeric
	}
}

// getNumericZero returns a typed numeric zero value, or int64(0) if the type V is any.
func getNumericZero[V any]() (V, error) {
	var zero V
	if any(zero) == nil {
		var zeroInt64 int64 = 0
		return any(zeroInt64).(V), nil
	}
	_, _, err := modifyNumericValue(zero, 0)
	if err != nil {
		return zero, err
	}
	return zero, nil
}

func (s *shard[K, V]) increment(key K, delta int64, now time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, exists := s.items[key]
	var current V
	if exists {
		if e.isExpired(now, s.defaultTTI) {
			s.remove(key, ReasonExpired)
			exists = false
		} else {
			current = e.value
		}
	}

	if !exists {
		cz, err := getNumericZero[V]()
		if err != nil {
			return 0, err
		}
		current = cz
	}

	newVal, intVal, err := modifyNumericValue(current, delta)
	if err != nil {
		return 0, err
	}

	if exists {
		e.value = newVal
		e.lastAccess = now
	} else {
		s.items[key] = &entry[V]{
			value:      newVal,
			expiresAt:  time.Time{},
			lastAccess: now,
		}
		if s.defaultTTL > 0 {
			s.items[key].expiresAt = now.Add(s.defaultTTL)
		}
		if s.policy != nil {
			s.policy.OnInsert(key)
		}
	}
	return intVal, nil
}

func (c *cacheImpl[K, V]) Increment(key K, delta int64) (int64, error) {
	return c.getShard(key).increment(key, delta, time.Now())
}

func (c *cacheImpl[K, V]) Decrement(key K, delta int64) (int64, error) {
	return c.getShard(key).increment(key, -delta, time.Now())
}
