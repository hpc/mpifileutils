# Contributing to mpiFileUtils

*Thank you for taking the time to contribute!*

## Table of Contents
[Resources](#resources)

[How To Contribute](#how-to-contribute)
  * [Reporting an Issue & Feature Suggestions](#reporting-an-issue--feature-suggestions)
  * [Pull Requests](#pull-requests)
  * [License](#license)
  * [Contributor's Declaration](#contributors-declaration)

## Resources
[mpiFileUtils Google Group](https://groups.google.com/forum/#!forum/mpifileutils)

## How To Contribute

### Reporting an Issue & Feature Suggestions
You can find a list of all outstanding issues and feature requests on our Github
[issue tracker](https://github.com/hpc/mpifileutils/issues). Before creating a new
issue or feature suggestion, ensure that:
* The issue or feature request hasn't been addressed in a newer version of mpiFileUtils.
* That the issue or feature request doesn't already exist in the issue tracker.

If the issue or feature request already exists, please provide additional
information if possible.

When creating a new issue, please provide at least the following information:
* mpiFileUtils Version
* Linux Distribution & Version
* Steps to reproduce the problem
* Information from various system logs (i.e. call stacks, dmesg output)

### Pull Requests
If you would like us to consider a change to mpiFileUtils, feel free to submit
a pull request. Your pull request must meet the following criteria before
it can be merged.

* Your pull request must be based on the current master branch and
apply without conflicts.
* Please try to limit pull requests to a single commit which resolves
one issue.
* Make sure your commit messages have a `Signed-off-by` line. See
[Contributor's Declaration](#contributors-declaration) for more information.
* For large pull requests, consider structuring your changes as a stack of
logically independent patches which build on each other.  This makes large
changes easier to review and approve which speeds up the merging process.
* Try to keep pull requests simple. Simple code with comments is much easier
to review and approve.
* Test cases should be provided when appropriate.
* Your pull request must pass automated testing.
* All proposed changes must be approved by an mpiFileUtils project member.

### License
mpiFileUtils is distributed under the New BSD License with a few additional notices.
Note that the phrase "above copyright notice" in the license text refers to the
current list of copyrights that appears in the
[license](https://github.com/hpc/mpifileutils/blob/master/LICENSE) file found in
the mpiFileUtils master branch.

### Contributor's Declaration
mpiFileUtils has adopted the signed-off-by process as described in Section
11 of the Linux kernel document on
[Submitting Patches](https://www.kernel.org/doc/html/latest/process/submitting-patches.html).
Each proposed contribution to the mpiFileUtils code base must include the text
"Signed-off-by:" followed by the contributor's name and email address. This is
the developer's certification that they have the right to submit the patch for
inclusion into the code base and indicates agreement to the Developer's
Certificate of Origin:

"By making a contribution to this project, I certify that:
1. The contribution was created in whole or in part by me and I have the right
to submit it under the open source license indicated in the file; or
2. The contribution is based upon previous work that, to the best of my knowledge,
is covered under an appropriate open source license and I have the right under
that license to submit that work with modifications, whether created in whole or
in part by me, under the same open source license (unless I am permitted to submit
under a different license), as indicated in the file; or
3. The contribution was provided directly to me by some other person who certified
(1), (2) or (3) and I have not modified it.
4. I understand and agree that this project and the contribution are public and
that a record of the contribution (including all personal information I submit
with it, including my sign-off) is maintained indefinitely and may be
redistributed consistent with this project or the open source license(s) involved."

After reading and agreeing to the Contributor's Declaration, include your
Signed-off-by text like the following as the last line in your commit message:

```
Signed-off-by: Random J Developer <random@developer.example.org>
```

Proposed contributions failing to include a "Signed-off-by:" certification will
not be accepted into mpiFileUtils. The maintainers reserve the right to revert
any commit made without the required certification.
