# Usa una imagen base de Nginx
FROM nginx:alpine

# Copia los archivos estáticos al directorio de Nginx
COPY ./static /usr/share/nginx/html

# Expone el puerto 80
EXPOSE 80

# Comando para iniciar Nginx
CMD ["nginx", "-g", "daemon off;"]
