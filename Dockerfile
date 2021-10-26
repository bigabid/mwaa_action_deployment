FROM python:3.8-alpine

LABEL "com.github.actions.name"="MWAA Deploy"
LABEL "com.github.actions.description"="Deploy MWAA Enviroment"
LABEL "com.github.actions.icon"="refresh-cw"
LABEL "com.github.actions.color"="green"

LABEL version="0.0.1"
LABEL repository="https://github.com/bigabid/mwaa_action_deployment.git"
LABEL homepage="https://github.com/bigabid/mwaa_action_deployment.git"
LABEL maintainer="Rotem Levi <rotem.l@bigabid.com>"
  
WORKDIR /app

COPY . .

RUN addgroup --gid 1000 mwaa && \
    adduser --system --shell=/sbin/nologin --uid 1000 -group mwaa

RUN chown -R mwaa: ./

USER mwaa

ADD requirements.txt /requirements.txt

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --user --upgrade pip wheel
RUN python3 -m pip install --user -r requirements.txt

ENTRYPOINT ["/entrypoint.sh"]


 
