// list of owner who can control dapr-bot workflow
// TODO: Read owners from OWNERS file.
const owners = [
    'addjuarez',
    'amuluyavarote',
    'artursouza',
    'berndverst',
    'daixiang0',
    'DeepanshuA',
    'halspang',
    'ItalyPaleAle',
    'jjcollinge',
    'joshvanl',
    'mikeee',
    'msfussell',
    'mukundansundar',
    'pkedy',
    'pravinpushkar',
    'robertojrojas',
    'RyanLettieri',
    'shivamkm07',
    'shubham1172',
    'skyao',
    'Taction',
    'tmacam',
    'yaron2',
    'yash-nisar',
]

const docsIssueBodyTpl = (
    issueNumber
) => `This issue was automatically created by \
[Dapr Bot](https://github.com/dapr/dapr/blob/master/.github/workflows/dapr-bot.yml) because a \"documentation required\" label \
was added to dapr/components-contrib#${issueNumber}. \n\n\
TODO: Add more details as per [this template](.github/ISSUE_TEMPLATE/new-content-needed.md).`

const newComponentBodyTpl = (
    issueNumber
) => `This issue was automatically created by \
[Dapr Bot](https://github.com/dapr/dapr/blob/master/.github/workflows/dapr-bot.yml) because a \"new component\" label \
was added to dapr/components-contrib#${issueNumber}. \n\n\

Please register the component in [cmd/daprd/components](https://github.com/dapr/dapr/tree/master/cmd/daprd/components), \
similar to the ones in the folder (one file per component).`

module.exports = async ({ github, context }) => {
    if (
        context.eventName == 'issue_comment' &&
        context.payload.action == 'created'
    ) {
        await handleIssueCommentCreate({ github, context })
    } else if (
        context.eventName == 'issues' &&
        context.payload.action == 'labeled'
    ) {
        await handleIssueOrPrLabeled({ github, context })
    } else {
        console.log(`[main] event ${context.eventName} not supported, exiting.`)
    }
}

/**
 * Handle issue comment create event.
 */
async function handleIssueCommentCreate({ github, context }) {
    const payload = context.payload
    const issue = context.issue
    const username = (context.actor || '').toLowerCase()
    const isFromPulls = !!payload.issue.pull_request
    const commentBody = payload.comment.body

    if (!commentBody) {
        console.log(
            '[handleIssueCommentCreate] comment body not found, exiting.'
        )
        return
    }
    const command = commentBody.split(' ')[0]

    // Commands that can be executed by anyone.
    if (command === '/assign') {
        await cmdAssign(github, issue, username, isFromPulls)
        return
    }

    // Commands that can only be executed by owners.
    if (owners.map((v) => v.toLowerCase()).indexOf(username) < 0) {
        console.log(
            `[handleIssueCommentCreate] user ${username} is not an owner, exiting.`
        )
        return
    }

    switch (command) {
        case '/ok-to-test':
            await cmdOkToTest(github, issue, isFromPulls)
            break
        case command.match(/^\/rerun \d+/)?.input:
            const workflowrunid = command.match(/\d+/)[0];
            await rerunWorkflow(github, issue, workflowrunid)
            break
        default:
            console.log(
                `[handleIssueCommentCreate] command ${command} not found, exiting.`
            )
            break
    }
}

/**
 * Handle issue or PR labeled event.
 */
async function handleIssueOrPrLabeled({ github, context }) {
    const payload = context.payload
    const label = payload.label.name
    const issueNumber = payload.issue.number

    // This should not run in forks.
    if (context.repo.owner !== 'dapr') {
        console.log(
            '[handleIssueOrPrLabeled] not running in dapr repo, exiting.'
        )
        return
    }

    // Authorization is not required here because it's triggered by an issue label event.
    // Only authorized users can add labels to issues.
    if (label == 'documentation required') {
        // Open a new docs issue
        await github.rest.issues.create({
            owner: 'dapr',
            repo: 'docs',
            title: `New content needed for dapr/components-contrib#${issueNumber}`,
            labels: ['content/missing-information', 'created-by/dapr-bot'],
            body: docsIssueBodyTpl(issueNumber),
        })
    } else if (label == 'new component') {
        // Open a new dapr issue
        await github.rest.issues.create({
            owner: 'dapr',
            repo: 'dapr',
            title: `Component registration for dapr/components-contrib#${issueNumber}`,
            labels: ['area/components', 'created-by/dapr-bot'],
            body: newComponentBodyTpl(issueNumber),
        })
    } else {
        console.log(
            `[handleIssueOrPrLabeled] label ${label} not supported, exiting.`
        )
    }
}

/**
 * Assign the issue to the user who commented.
 * @param {*} github GitHub object reference
 * @param {*} issue GitHub issue object
 * @param {*} username GitHub user who commented
 * @param {boolean} isFromPulls is the workflow triggered by a pull request?
 */
async function cmdAssign(github, issue, username, isFromPulls) {
    if (isFromPulls) {
        console.log(
            '[cmdAssign] pull requests unsupported, skipping command execution.'
        )
        return
    } else if (issue.assignees && issue.assignees.length !== 0) {
        console.log(
            '[cmdAssign] issue already has assignees, skipping command execution.'
        )
        return
    }

    await github.rest.issues.addAssignees({
        owner: issue.owner,
        repo: issue.repo,
        issue_number: issue.number,
        assignees: [username],
    })
}

/**
 * Trigger e2e test for the pull request.
 * @param {*} github GitHub object reference
 * @param {*} issue GitHub issue object
 * @param {boolean} isFromPulls is the workflow triggered by a pull request?
 */
async function cmdOkToTest(github, issue, isFromPulls) {
    if (!isFromPulls) {
        console.log(
            '[cmdOkToTest] only pull requests supported, skipping command execution.'
        )
        return
    }

    // Get pull request
    const pull = await github.rest.pulls.get({
        owner: issue.owner,
        repo: issue.repo,
        pull_number: issue.number,
    })

    if (pull && pull.data) {
        // Get commit id and repo from pull head
        const testPayload = {
            pull_head_ref: pull.data.head.sha,
            pull_head_repo: pull.data.head.repo.full_name,
            command: 'ok-to-test',
            issue: issue,
        }

        // Fire repository_dispatch event to trigger certification test
        await github.rest.repos.createDispatchEvent({
            owner: issue.owner,
            repo: issue.repo,
            event_type: 'certification-test',
            client_payload: testPayload,
        })

        // Fire repository_dispatch event to trigger conformance test
        await github.rest.repos.createDispatchEvent({
            owner: issue.owner,
            repo: issue.repo,
            event_type: 'conformance-test',
            client_payload: testPayload,
        })

        // Fire repository_dispatch event to trigger unit tests for other architectures and OS
        await github.rest.repos.createDispatchEvent({
            owner: issue.owner,
            repo: issue.repo,
            event_type: 'build-all',
            client_payload: testPayload,
        })

        console.log(
            `[cmdOkToTest] triggered certification and conformance tests for ${JSON.stringify(
                testPayload
            )}`
        )
    }
}

/**
 * Rerun all failed jobs of a given workflow run ID.
 * @param {*} github GitHub object reference
 * @param {*} issue GitHub issue object
 * @param {int} workflowrunid the workflow run ID for which to rerun all failed jobs
 */
async function rerunWorkflow(github, issue, workflowrunid) {
    // Rerun all failed jobs of the specified workflow run
    const pull = await github.rest.actions.reRunWorkflowFailedJobs({
       owner: issue.owner,
       repo: issue.repo,
       run_id: workflowrunid,
    });
}
