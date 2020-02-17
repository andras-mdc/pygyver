# load vars in repo
set -a
source vars/build/default/default.env
source vars/build/local/local.env
set +a

# get latest git tag or base image from github
export BASE_IMAGE_GIT_TAG=$(git ls-remote --tags "git@github.com:madedotcom/analytics-python-docker-base.git" | grep ${PY_VERSION} | awk "{print $2}" | cut -d "/" -f 3 | sort -nr -b -t - -k 2 | awk "NR==1")