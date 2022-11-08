# Cron certification testing

This project aims to test the cron component under various conditions.

## Test plan

### Input Binding Test

* Test cron trigger with deadline limits  
> Example: a cron trigger with a schedule `@every 1s` should trigger 10 times in the next 10 seconds
* Test cron trigger with different schedules and cron formats  
> Test cron trigger with schedule written in the standard crontab format `*/3 * * * * *` and another cron trigger with schedule written in non-standard macro format `@every 1s`
* Test cron trigger on app restart
> Check if cron will still trigger the app in case the app listening to cron trigger crashes and restarts
* Test cron trigger on sidecar restart
> Check if the app is still triggered by cron in case of dapr sidecar restart.

### Output Binding Test

* Test cron delete operation  
> For each of the test cases above, after asserting that cron has triggered correctly, invoke cron binding's delete operation. Wait for some time and assert that cron does not trigger again.
