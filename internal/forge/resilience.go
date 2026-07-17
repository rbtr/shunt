package forge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	processLimiterMu sync.Mutex
	processLimiter   *tokenBucket
)

func sharedLimiter(cfg Config) *tokenBucket {
	processLimiterMu.Lock()
	defer processLimiterMu.Unlock()
	if processLimiter == nil {
		processLimiter = newTokenBucket(cfg.RatePerSecond, cfg.RateBurst)
	}
	return processLimiter
}

type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	return &tokenBucket{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   time.Now(),
	}
}

func (b *tokenBucket) wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		now := time.Now()
		b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		b.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type outageCircuit struct {
	mu          sync.Mutex
	until       time.Time
	backoff     time.Duration
	probing     bool
	rateLimited time.Time
}

func (c *Client) request(ctx context.Context, method, path string, body any) ([]byte, int, error) {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
	}

	for attempt := 0; ; attempt++ {
		if err := c.allow(ctx); err != nil {
			return nil, 0, err
		}
		if err := c.limiter.wait(ctx); err != nil {
			return nil, 0, err
		}
		if err := c.allow(ctx); err != nil {
			return nil, 0, err
		}

		data, status, header, err := c.send(ctx, method, c.apiBase+path, payload, true)
		if err != nil {
			if ctx.Err() != nil {
				return nil, 0, ctx.Err()
			}
			if c.retryableMethod(method) && attempt < c.cfg.RetryAttempts {
				if err := sleepContext(ctx, c.retryDelay(attempt)); err != nil {
					return nil, 0, err
				}
				continue
			}
			return nil, 0, c.unavailableAfterFailure(err)
		}

		if status >= 200 && status < 300 {
			return data, status, nil
		}
		statusErr := &httpStatusError{method: method, path: path, status: status, body: data}
		if status == http.StatusTooManyRequests {
			retryAt := c.setRateLimited(time.Now(), header)
			return nil, status, unavailable(statusErr, retryAt)
		}
		if status >= 500 && status < 600 {
			if c.retryableMethod(method) && attempt < c.cfg.RetryAttempts {
				if err := sleepContext(ctx, c.retryDelay(attempt)); err != nil {
					return nil, 0, err
				}
				continue
			}
			return nil, status, c.unavailableAfterFailure(statusErr)
		}
		return nil, status, statusErr
	}
}

func (c *Client) retryableMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

func (c *Client) retryDelay(attempt int) time.Duration {
	delay := c.cfg.RetryInitial
	for range attempt {
		if delay >= c.cfg.RetryMax/2 {
			return c.cfg.RetryMax
		}
		delay *= 2
	}
	return min(delay, c.cfg.RetryMax)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) send(ctx context.Context, method, endpoint string, payload []byte, auth bool) ([]byte, int, http.Header, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, 0, nil, err
	}
	if auth {
		req.Header.Set("Authorization", "token "+c.token)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, nil, err
	}
	data, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return nil, 0, resp.Header, readErr
	}
	if closeErr != nil {
		return nil, 0, resp.Header, closeErr
	}
	return data, resp.StatusCode, resp.Header, nil
}

func (c *Client) allow(ctx context.Context) error {
	for {
		now := time.Now()
		c.outage.mu.Lock()
		if now.Before(c.outage.until) {
			retryAt := c.outage.until
			c.outage.mu.Unlock()
			return unavailable(nil, retryAt)
		}
		if !c.outage.until.IsZero() {
			if c.outage.probing {
				retryAt := c.outage.until
				c.outage.mu.Unlock()
				return unavailable(nil, retryAt)
			}
			c.outage.probing = true
			c.outage.mu.Unlock()

			err := c.probe(ctx)

			c.outage.mu.Lock()
			c.outage.probing = false
			if err != nil {
				retryAt := c.openOutageLocked(time.Now())
				c.outage.mu.Unlock()
				return unavailable(err, retryAt)
			}
			c.outage.until = time.Time{}
			c.outage.backoff = c.cfg.OutageInitial
			if time.Now().Before(c.outage.rateLimited) {
				retryAt := c.outage.rateLimited
				c.outage.mu.Unlock()
				return unavailable(ErrRateLimited, retryAt)
			}
			c.outage.mu.Unlock()
			return nil
		}
		if now.Before(c.outage.rateLimited) {
			retryAt := c.outage.rateLimited
			c.outage.mu.Unlock()
			return unavailable(ErrRateLimited, retryAt)
		}
		c.outage.mu.Unlock()
		return nil
	}
}

func (c *Client) probe(ctx context.Context) error {
	if err := c.limiter.wait(ctx); err != nil {
		return err
	}
	data, status, header, err := c.send(ctx, http.MethodGet, c.instanceBase+"/api/healthz", nil, false)
	if err != nil {
		return err
	}
	if status == http.StatusTooManyRequests {
		c.setRateLimited(time.Now(), header)
		return &httpStatusError{method: http.MethodGet, path: "/api/healthz", status: status, body: data}
	}
	if status >= 200 && status < 300 {
		return nil
	}
	if status != http.StatusNotFound {
		return &httpStatusError{method: http.MethodGet, path: "/api/healthz", status: status, body: data}
	}

	if err := c.limiter.wait(ctx); err != nil {
		return err
	}
	data, status, header, err = c.send(ctx, http.MethodGet, c.apiBase+"/version", nil, true)
	if err != nil {
		return err
	}
	if status == http.StatusTooManyRequests {
		c.setRateLimited(time.Now(), header)
	}
	if status < 200 || status >= 300 {
		return &httpStatusError{method: http.MethodGet, path: "/version", status: status, body: data}
	}
	return nil
}

func (c *Client) unavailableAfterFailure(cause error) error {
	c.outage.mu.Lock()
	retryAt := c.openOutageLocked(time.Now())
	c.outage.mu.Unlock()
	return unavailable(cause, retryAt)
}

func (c *Client) openOutageLocked(now time.Time) time.Time {
	c.outage.until = now.Add(c.outage.backoff)
	if c.outage.backoff >= c.cfg.OutageMax/2 {
		c.outage.backoff = c.cfg.OutageMax
	} else {
		c.outage.backoff *= 2
	}
	return c.outage.until
}

func (c *Client) setRateLimited(now time.Time, header http.Header) time.Time {
	retryAt := now.Add(c.cfg.RetryInitial)
	if retryAfter := strings.TrimSpace(header.Get("Retry-After")); retryAfter != "" {
		if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
			retryAt = now.Add(time.Duration(seconds) * time.Second)
		} else if when, err := http.ParseTime(retryAfter); err == nil && when.After(now) {
			retryAt = when
		}
	}

	c.outage.mu.Lock()
	if retryAt.After(c.outage.rateLimited) {
		c.outage.rateLimited = retryAt
	}
	retryAt = c.outage.rateLimited
	c.outage.mu.Unlock()
	return retryAt
}

func unavailable(cause error, retryAt time.Time) error {
	return &UnavailableError{Cause: cause, RetryAt: retryAt}
}

type httpStatusError struct {
	method string
	path   string
	status int
	body   []byte
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%s %s: http %d: %s", e.method, e.path, e.status, strings.TrimSpace(string(e.body)))
}

func (e *httpStatusError) Is(target error) bool {
	return target == ErrRateLimited && e.status == http.StatusTooManyRequests
}
