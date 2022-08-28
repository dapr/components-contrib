// we need two additional imports.
// These are created by github and are especially built
// for github actions.
// You can find more information here:
// https://github.com/actions/toolkit
const core = require("@actions/core");
const github = require("@actions/github");
const fs = require('fs');

// Main function of this action: read in the files and produce the comment.
// The async keyword makes the run function controlled via
// an event loop - which is beyond the scope of the blog.
// Just remember: we will use a library which has asynchronous
// functions, so we also need to call them asynchronously.

async function ls(path) {
    const dir = await fs.promises.opendir(path)
    for await (const dirent of dir) {
      console.log(dirent.name)
    }
  }

async function calculateTotalCoveragePercentage(github, context, covDir, threshold) {
  // The github module has a member called "context",
  // which always includes information on the action workflow
  // we are currently running in.
  // For example, it let's us check the event that triggered the workflow.
//   if (github.context.eventName !== "pull_request") {
    // The core module on the other hand let's you get
    // inputs or create outputs or control the action flow
    // e.g. by producing a fatal error
//     core.setFailed("Can only run on pull requests!");
//     return;
//   }
  // get the inputs of the action. The "token" input
  // is not defined so far - we will come to it later.
//   const githubToken = core.getInput("token");
  // const covDir = core.getInput("covDir");


  const dir = await fs.promises.opendir(covDir)
    for await (const dirent of dir) {
      console.log(dirent.name)
    }


//   const oldBenchmarkFileName = core.getInput("comparison_json_file");
  // Now read in the files with the function defined above
//   const benchmarks = readJSON(benchmarkFileName);
//   let oldBenchmarks = undefined;
//   if(oldBenchmarkFileName) {
//     try {
//       oldBenchmarks = readJSON(oldBenchmarkFileName);
//     } catch (error) {
//       console.log("Can not read comparison file. Continue without it.");
//     }
//   }
  // and create the message
//   const message = createMessage(benchmarks, oldBenchmarks);
  // output it to the console for logging and debugging
//   console.log(message);
  // the context does for example also include information
  // in the pull request or repository we are issued from
//   const context = github.context;
//   const repo = context.repo;
//   const pullRequestNumber = context.payload.pull_request.number;
  // The Octokit is a helper, to interact with
  // the github REST interface.
  // You can look up the REST interface
  // here: https://octokit.github.io/rest.js/v18
//   const octokit = github.getOctokit(githubToken);
  // Get all comments we currently have...
  // (this is an asynchronous function)
//   const { data: comments } = await octokit.issues.listComments({
//     ...repo,
//     issue_number: pullRequestNumber,
//   });
//   // ... and check if there is already a comment by us
//   const comment = comments.find((comment) => {
//     return (
//       comment.user.login === "github-actions[bot]" &&
//       comment.body.startsWith("## Result of Benchmark Tests\n")
//     );
//   });
  // If yes, update that
//   if (comment) {
//     await octokit.issues.updateComment({
//       ...repo,
//       comment_id: comment.id,
//       body: message
//     });
//   // if not, create a new comment
//   } else {
//     await octokit.issues.createComment({
//       ...repo,
//       issue_number: pullRequestNumber,
//       body: message
//     });
//   }
}
// Our main method: call the run() function and report any errors
// run().catch(error => core.setFailed("Workflow failed! " + error.message));
exports.calculateTotalCoveragePercentage = calculateTotalCoveragePercentage(certTest_covFiles);