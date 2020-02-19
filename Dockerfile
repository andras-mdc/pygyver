ARG IMAGE_REGISTRY
ARG BASE_IMAGE_GIT_TAG
FROM ${IMAGE_REGISTRY}/py-base:${BASE_IMAGE_GIT_TAG:-snapshot}

ENV TERM=xterm-256color  

WORKDIR /code

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip3 install -r requirements.txt

COPY pygyver src
COPY tests src/tests

COPY entrypoint.sh /bin/entrypoint

ENTRYPOINT ["/bin/entrypoint"]
