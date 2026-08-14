FROM node:22-bullseye

LABEL authors="Onye Viki"

RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    build-essential \
    make \
    g++ \
    libstdc++6 \
    libgcc-s1 \
    libsrtp2-1 \
    libssl1.1 \
    libuv1 \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN python3 --version && \
    python3 -m pip --version && \
    uname -m && \
    node -p "process.arch"

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 4000
EXPOSE 20000/tcp
EXPOSE 20000/udp
EXPOSE 10000-59999/udp

CMD ["npm", "run", "dev"]