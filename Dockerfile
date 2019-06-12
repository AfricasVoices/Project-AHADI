FROM python:3.6-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Install pyflame (for statistical profiling) if this script is run with PROFILE_CPU flag
ARG INSTALL_CPU_PROFILER="false"
RUN if [ "$INSTALL_CPU_PROFILER" = "true" ]; then \
        apt-get update && apt-get install -y autoconf automake autotools-dev g++ pkg-config python-dev python3-dev libtool make && \
        git clone https://github.com/uber/pyflame.git /pyflame && cd /pyflame && git checkout "v1.6.7" && \
        ./autogen.sh && ./configure && make && make install && \
        rm -rf /pyflame; \
    fi

# Make a directory for private credentials files
RUN mkdir /credentials

# Make a directory for intermediate data
RUN mkdir /data

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Copy the rest of the project
#ADD code_schemes/*.json /app/code_schemes/
ADD src /app/src
ADD fetch_raw_data.py /app
#ADD fetch_recovered_data.py /app
#ADD fetch_flow_definitions.py /app
#ADD generate_outputs.py /app
