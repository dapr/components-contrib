# Cron

This package is a fork of [`github.com/robfig/cron/v3@v3.0.0`](https://github.com/robfig/cron/) which adds support for mocking the time using [`github.com/benbjohnson/clock`](https://github.com/benbjohnson/clock)

See [LICENSE](./LICENSE) for the license of the original package.

## Using Cron with Mock Clock

```go
import "github.com/benbjohnson/clock"

clk := clock.NewMock()
c := cron.New(cron.WithClock(clk))
c.AddFunc("@every 1h", func() {
 fmt.Println("Every hour")
})
c.Start()
clk.Add(1 * time.Hour)
```
