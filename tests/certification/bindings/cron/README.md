# Cron certification testing

This project aims to test the cron component under various conditions.

## Test plan

* Test cron trigger with deadline limits
> Example: a cron trigger with a schedule `@every 1s` should trigger 10 times in the next 10 seconds
* Test cron trigger with different schedules and cron formats
> Test cron trigger with schedule written in the standard crontab format `*/3 * * * * *` and another cron trigger with schedule written in non-standard macro format `@every 1s`
* Test cron triggers having same route
> Check if two cron triggers having different schedules of `@every 1s` and `@every 3s` respectively and having same app route `/cron` should both trigger the app correctly
* Test cron trigger on app restart
> Check if cron will still trigger the app in case the app listening to cron trigger crashes and restarts
* Test cron trigger on sidecar restart
> Check if the app is still triggered by cron in case of dapr sidecar restart.

