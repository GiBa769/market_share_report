# How file updates flow to GitHub

When you request a file change, the repository does not update on GitHub automatically. The steps below describe how to turn a local edit into a visible change in the remote repo and keep the QAQC pipeline reproducible.

## 1) Edit and verify locally
- Apply the change in the working tree (e.g., update `docs/etl_optimization_plan.md` or any `src/` module).
- Run the relevant checks/tests before committing so the pipeline logic remains intact.

## 2) Stage and commit the change
- Use `git status` to confirm which files were modified.
- Stage only the intended files (`git add <paths>`), then create a commit with a clear message (`git commit -m "Describe the change"`).

## 3) Push and open a PR
- Push the branch to the remote (`git push origin <branch>`).
- Open a pull request so reviewers can see the diff, comments, and CI results. The code becomes part of the GitHub repository only after the PR is merged.

## 4) Keep artifacts lean
- Avoid manual copy/paste of large raw outputs into GitHub; store only the code/config and minimal example data needed for QAQC.
- If large intermediate outputs are required for debugging, keep them local or in object storage and link them from the PR description instead of committing them.

Following this flow ensures updates are tracked, reviewable, and do not bloat the repo with unnecessary data exports.
