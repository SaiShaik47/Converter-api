FROM node:20-bookworm-slim

# Install LibreOffice (Excel->PDF) + Java (Tabula PDF->Excel) + OCR deps + fonts + qpdf + 7z (protect/unlock)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice-core libreoffice-calc \
    fonts-dejavu fonts-liberation \
    default-jre wget qpdf p7zip-full \
    tesseract-ocr tesseract-ocr-eng ocrmypdf ghostscript \
  && rm -rf /var/lib/apt/lists/*

# Download Tabula jar
RUN mkdir -p /opt/tabula \
  && wget -O /opt/tabula/tabula.jar https://github.com/tabulapdf/tabula-java/releases/download/v1.0.5/tabula-1.0.5-jar-with-dependencies.jar

ENV TABULA_JAR=/opt/tabula/tabula.jar

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY . .
EXPOSE 3000
CMD ["npm", "start"]
