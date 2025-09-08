# ============================================================
# Unir todos los CSV y GeoJSON del directorio actual
# ============================================================

## === 1. CONFIGURACIÓN: nombres de archivos de salida ========
output_csv      <- "incendios-ca-historicos-1980-2024.csv"     # archivo final CSV
output_geojson  <- "incendios-ca-historicos-1980-2024.geojson" # archivo final GeoJSON

## === 2. CARGAR LIBRERÍAS =====================================
suppressPackageStartupMessages({
  library(readr)   # leer/escribir CSV
  library(dplyr)   # bind_rows
  library(sf)      # leer/escribir GeoJSON
})

## === 3. PROCESAR CSV ========================================
message("🔍 Buscando archivos CSV…")
csv_files <- list.files(pattern = "\\.csv$", ignore.case = TRUE) |> sort()

if (length(csv_files) == 0) {
  stop("❌ No se encontraron archivos CSV en el directorio.")
}

message("📑 Leyendo y concatenando CSV:")
csv_list <- lapply(csv_files, function(f) {
  message("   • ", f)
  read_csv(
    f, 
    show_col_types = FALSE,
    col_types = cols(
      .default = col_guess(),
      FIRE_NUM  = col_character(),
      COMMENTS = col_character(),
    )
  )
})

csv_combined <- bind_rows(csv_list)

message("💾 Escribiendo archivo combinado → ", output_csv)
write_csv(csv_combined, output_csv)
message("✅ CSV consolidado creado con éxito.")

## === 4. PROCESAR GEOJSON ====================================
message("\n🔍 Buscando archivos GeoJSON…")
geo_files <- list.files(pattern = "\\.geojson$", ignore.case = TRUE) |> sort()

if (length(geo_files) == 0) {
  stop("❌ No se encontraron archivos GeoJSON en el directorio.")
}

message("🌐 Leyendo y combinando GeoJSON:")
geo_list <- lapply(geo_files, function(f) {
  message("   • ", f)
  st_read(f, quiet = TRUE)  # cada archivo como objeto sf
})

geo_combined <- do.call(rbind, geo_list)

message("💾 Escribiendo archivo combinado → ", output_geojson)
st_write(geo_combined, output_geojson, delete_dsn = TRUE, quiet = TRUE)
message("✅ GeoJSON consolidado creado con éxito.")

## === 5. FIN =================================================
message("\n🎉 Proceso completado satisfactoriamente.")
