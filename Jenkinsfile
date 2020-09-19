#!groovy

node {
    def MilevaDB_TEST_BRANCH = "master"
    def EinsteinDB_BRANCH = "master"
    def FIDel_BRANCH = "master"

    fileLoader.withGit('git@github.com:whtcorpsinc/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/WHTCORPS INC_milevadb_branch.groovy').call(MilevaDB_TEST_BRANCH, EinsteinDB_BRANCH, FIDel_BRANCH)
    }
}
