FROM node:24.13.0 as build

WORKDIR /app

COPY ./app3/package.json ./
RUN npm install

COPY ./app3 .

RUN npm run build

FROM nginx:1.28

COPY --from=build /app/dist /usr/share/nginx/html
COPY ./default.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]