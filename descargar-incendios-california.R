# Descarga de datos de incendios históricos de California
# Fuente de los datos: 
#   The California Department of Forestry and Fire Protection (CAL FIRE)
#   (https://www.fire.ca.gov/)


# Cargar paquetes
library(dplyr)
library(lubridate)
library(httr)
library(jsonlite)
library(sf)
library(terra)
library(rnaturalearth)
library(KrigR)


# DATOS DE ENTRADA

# --- Argumentos CLI: --year-start N --year-end N  (o year_start=N year_end=N)
args <- commandArgs(trailingOnly = TRUE)

.get_arg_val <- function(args, long_flag, key = NULL) {
  # Soporta:  --year-start 1960   y   year_start=1960
  val <- NA_character_
  if (length(args) >= 2 && any(args == long_flag)) {
    idx <- which(args == long_flag)[1]
    if (idx < length(args)) val <- args[idx + 1]
  }
  if (is.na(val) && !is.null(key)) {
    kv <- grep(paste0("^", key, "="), args, value = TRUE)
    if (length(kv)) val <- sub(paste0("^", key, "="), "", kv[1])
  }
  val
}

ys <- .get_arg_val(args, "--year-start", "year_start")
ye <- .get_arg_val(args, "--year-end",   "year_end")
if (!is.na(ys)) year_start <- as.integer(ys)
if (!is.na(ye)) year_end   <- as.integer(ye)

# Recalcular label con los argumentos (o defaults si no llegaron)
label <- sprintf("%d-%d", year_start, year_end)

# Máximo de registros a procesar (NA = todos)
max_records <- NA  # ej. 100 si solo se desea obtener un máximo de 100 incendios

# Directorios por año (cada job usa su propio tmp/<label>)
dir_base <- getwd()
dir_tmp  <- file.path(dir_base, "tmp", label)   # <- único por año
dir_out  <- file.path(dir_base, "salidas")      # <- común; archivos incluyen el label

dir.create(dir_tmp, recursive = TRUE, showWarnings = FALSE)
dir.create(dir_out, recursive = TRUE, showWarnings = FALSE)

# Salidas (en salidas/)
output_geojson <- file.path(dir_out, sprintf("incendios_ca_%s.geojson", label))
output_csv     <- file.path(dir_out, sprintf("incendios_ca_%s.csv",     label))

# URL base del API de CAL FIRE
base_url <- "https://services1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis/rest/services/California_Historic_Fire_Perimeters/FeatureServer/0/query"

# Lectura del usuario y llave del API de CDS (Copernicus Data Store)
# El archivo .Renviron debe tener el siguiente contenido:
# CDSAPI_USER=correo@ejemplo.com
# CDSAPI_KEY=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
readRenviron(".Renviron")

# Variables climáticas de ERA5
VARIABLES <- c(
  "2m_temperature"
  # "skin_temperature",
  # "total_precipitation",
  # "soil_temperature_level_1",
  # "10m_u_component_of_wind",
  # "10m_v_component_of_wind",
  # "leaf_area_index_high_vegetation",
  # "leaf_area_index_low_vegetation"
)

# Prefijos de nombres de columnas para almacenar variables climáticas de ERA5
COLUMN_PREFIXES <- c(
  "TEMPERATURE_2M_ALARM_DATE"
  # "SKIN_TEMPERATURE_ALARM_DATE",
  # "PRECIPITATION_ALARM_DATE",
  # "soil_temperature_level_1",
  # "10m_u_component_of_wind",
  # "10m_v_component_of_wind",
  # "leaf_area_index_high_vegetation",
  # "leaf_area_index_low_vegetation"
)

# Funciones de agregación
AGGREGATE_FUNCTIONS <- c(
  "min", "max", "mean"
)

# Reglas de conversión de unidades
unit_conversion_rules <- list(
  "2m_temperature"           = function(x) x - 273.15, # K → °C
  "skin_temperature"         = function(x) x - 273.15, # K → °C
  "soil_temperature_level_1" = function(x) x - 273.15, # K → °C
  "total_precipitation"      = function(x) x * 1000    # m → mm
)


# VALIDACIONES RÁPIDAS

# Validar años
current_year <- year(Sys.Date())
if (year_start < 1878 || year_start > current_year || 
    year_end < 1878 || year_end > current_year ||
    year_start > year_end) {
  stop("Rango de años inválido. Los años deben estar entre 1878 y ", current_year, 
       " y el año inicial debe ser menor o igual al año final.")
}


# PREPROCESAMIENTO

# Crear cláusula where para filtrar por rango de años
where_clause <- sprintf(
  "(YEAR_ >= %d AND YEAR_ <= %d) OR (ALARM_DATE >= '%d-01-01' AND ALARM_DATE <= '%d-12-31')",
  year_start, 
  year_end,
  year_start,
  year_end
)

# (Re)crear subdirectorios limpios en cada ejecución
invisible(sapply(
  c(dir_tmp, dir_out),
  function(x) {
    if (dir.exists(x)) unlink(x, recursive = TRUE, force = TRUE)
    dir.create(x, recursive = TRUE)
  }
))

# Obtener el polígono de California
us_states <- ne_states(
  country = "United States of America",
  returnclass = "sf"
)
california <- us_states[us_states$name == "California", ]
california <- st_transform(california, 4326)

# Vector nombrado (xmin, ymin, xmax, ymax)
bb <- st_bbox(california)


# PROCESAMIENTO

cat("Descargando datos de incendios desde", year_start, "hasta", year_end, "...\n")

# Función para obtener el conteo total de registros
get_record_count <- function() {
  params <- list(
    where = where_clause,
    returnCountOnly = "true",
    f = "json"
  )
  
  response <- GET(url = base_url, query = params)
  content <- content(response, as = "text", encoding = "UTF-8")
  data <- fromJSON(content)
  return(data$count)
}

# Función para descargar los datos en lotes
download_fire_data <- function(offset = 0, batch_size = 1000) {
  cat("Descargando registros", offset + 1, "a", offset + batch_size, "...\n")
  
  params <- list(
    where = where_clause,
    outFields = "*",
    outSR = 4326,
    outFields = "*",
    geometryPrecision = 6,
    f = "geojson",
    resultOffset = offset,
    resultRecordCount = batch_size,
    orderByFields = "YEAR_ ASC, ALARM_DATE ASC"  # Ordenar por año y fecha
  )
  
  response <- GET(url = base_url, query = params)
  
  if (http_status(response)$category == "Success") {
    content <- content(response, as = "text", encoding = "UTF-8")
    return(content)
  } else {
    stop("Error en la descarga del lote ", offset + 1, "-", offset + batch_size)
  }
}

# Obtener el número total de registros
total_records <- get_record_count()
cat("Total de registros encontrados:", total_records, "\n")

# Obtener el número de registros a descargar (máximo permitido por el usuario)
records_to_fetch <- if (is.na(max_records) || max_records > total_records) {
  total_records
} else {
  max_records
}
cat("Se procesarán", records_to_fetch, "registros\n")

# Tamaño del lote
batch_size <- 2000
all_data <- list()

# Descargar los datos en lotes
for (offset in seq(0, records_to_fetch - 1, by = batch_size)) {
  
  size_this_batch <- min(batch_size, records_to_fetch - offset)
  
  batch_data <- download_fire_data(offset, size_this_batch)
  all_data   <- c(all_data, batch_data)
  
  Sys.sleep(1)
}

# Procesar los datos encontrados
cat("Procesando datos encontrados...\n")

# Función para procesar un lote de datos JSON
process_batch <- function(json_text) {
  # Crear un archivo temporal
  temp_file <- tempfile(fileext = ".geojson")
  writeLines(json_text, temp_file)
  
  tryCatch({
    # Leer el archivo GeoJSON
    sf_obj <- st_read(temp_file, quiet = TRUE)
    
    # Verificar si hay geometrías válidas
    if (all(st_is_empty(sf_obj))) {
      cat("Advertencia: No se encontraron geometrías válidas en el lote.\n")
      return(NULL)
    }
    
    # Asegurarse de que el CRS sea 4326 (WGS84)
    if (is.na(st_crs(sf_obj))) {
      st_crs(sf_obj) <- 4326
    }
    
    return(sf_obj)
    
  }, error = function(e) {
    cat("Error al procesar el lote:", e$message, "\n")
    return(NULL)
  }, finally = {
    # Limpiar el archivo temporal
    if (file.exists(temp_file)) file.remove(temp_file)
  })
}

# Procesar lotes
combined_sf <- NULL
for (i in seq_along(all_data)) {
  cat("Procesando lote", i, "de", length(all_data), "...\n")
  temp_sf <- process_batch(all_data[[i]])
  
  if (!is.null(temp_sf) && nrow(temp_sf) > 0) {
    if (is.null(combined_sf)) {
      combined_sf <- temp_sf
    } else {
      combined_sf <- tryCatch({
        rbind(combined_sf, temp_sf)
      }, error = function(e) {
        cat("Error al combinar lotes:", e$message, "\n")
        combined_sf  # Retorna lo que se tenía hasta ahora
      })
    }
    cat("  - Geometrías válidas:", sum(!st_is_empty(temp_sf)), "de", nrow(temp_sf), "\n")
  } else {
    cat("  - Lote", i, "no contiene geometrías válidas.\n")
  }
}

# Identificar columnas de fecha y hora (con 'date' o 'fecha' en el nombre)
date_cols <- grep("(?i)date|fecha", names(combined_sf), value = TRUE)

# Convertir columnas de fecha y hora a formato YYYY-MM-DD HH:MM:SS
if (length(date_cols) > 0) {
  for (col in date_cols) {
    combined_sf[[col]] <- as_datetime(combined_sf[[col]] / 1000, tz = "UTC")
  }
}

# Calcular el centroide de las geometrías y extraer coordenadas x e y
if (!is.null(combined_sf) && nrow(combined_sf) > 0) {
  cat("Calculando centroides ...\n")
  
  # Forzar las geometrías a MULTIPOLYGON
  combined_sf <- st_cast(combined_sf, "MULTIPOLYGON", warn = FALSE)
  
  # Corregir geometrías
  combined_sf <- st_make_valid(combined_sf)
  
  # Desactivar s2 para el cálculo planar del centroide
  old_s2 <- sf_use_s2(FALSE)  
  
  # Calcular centroide
  cent <- st_centroid(combined_sf)
  # cent <- st_point_on_surface(combined_sf)  
  
  # Extraer coordenadas x e y
  coords <- st_coordinates(cent)
  combined_sf$Lon <- coords[, 1]
  combined_sf$Lat <- coords[, 2]
  
  # Reactivar s2
  sf_use_s2(old_s2)
}


# ==== NUEVO BLOQUE: VARIABLES → COLUMN_PREFIXES × AGGREGATE_FUNCTIONS ====

# 0) Validaciones y preparación
if (length(VARIABLES) != length(COLUMN_PREFIXES)) {
  stop("VARIABLES y COLUMN_PREFIXES deben tener la misma longitud.")
}

# Asegurar columna de fecha de alarma (solo día)
if (!"ALARM_DAY" %in% names(combined_sf)) {
  combined_sf$ALARM_DAY <- as.Date(combined_sf$ALARM_DATE, tz = "UTC")
}

# Puntos (centroides) como SpatVector una sola vez
pts <- terra::vect(
  data.frame(id = seq_len(nrow(combined_sf)), Lon = combined_sf$Lon, Lat = combined_sf$Lat),
  geom = c("Lon", "Lat"),
  crs  = "EPSG:4326"
)

date_start_str <- sprintf("%d-01-01", year_start)
date_stop_str  <- sprintf("%d-12-31", year_end)

# Dado un nombre de variable ERA5, indicar si es de precipitación
.is_precip <- function(var) grepl("precip", var, ignore.case = TRUE)

# Mapear el nombre del agregador de salida y el FUN real para CDownloadS
# (para precipitación reemplazamos MEAN→SUM)
.map_agg <- function(var, agg) {
  if (.is_precip(var) && agg == "mean") {
    list(suffix = "SUM", fun = "sum")
  } else {
    list(suffix = toupper(agg), fun = agg)
  }
}

# Inicializar TODAS las columnas de salida que se generarán
for (i in seq_along(VARIABLES)) {
  var   <- VARIABLES[i]
  pref  <- COLUMN_PREFIXES[i]
  for (agg in AGGREGATE_FUNCTIONS) {
    m <- .map_agg(var, agg)
    colname <- paste0(pref, "_", m$suffix)
    if (!colname %in% names(combined_sf)) combined_sf[[colname]] <- NA_real_
  }
}

# 1) Bucle principal por variable y por función de agregación
for (i in seq_along(VARIABLES)) {
  var   <- VARIABLES[i]
  pref  <- COLUMN_PREFIXES[i]
  
  for (agg in AGGREGATE_FUNCTIONS) {
    m <- .map_agg(var, agg)         # m$suffix (columna), m$fun (KrigR::FUN)
    colname <- paste0(pref, "_", m$suffix)
    
    cat(sprintf("→ ERA5 %s | agregación diaria: %s → columna: %s\n", var, m$fun, colname))
    
    # 1.1) Descarga del raster diario con la agregación pedida
    ras <- tryCatch({
      KrigR::CDownloadS(
        Variable    = var,
        DataSet     = "reanalysis-era5-land",
        Type        = NA,
        DateStart   = date_start_str,
        DateStop    = date_stop_str,
        TZone       = "UTC",
        TResolution = "day",
        FUN         = m$fun,                 # "min","max","mean","sum"
        TStep       = 1,
        Extent      = bb,                    # bbox de California
        Dir         = dir_tmp,
        FileName    = sprintf("%s_%s_ca_diaria_%s", var, m$fun, label),
        API_User    = Sys.getenv("CDSAPI_USER"),
        API_Key     = Sys.getenv("CDSAPI_KEY"),
        Cores       = as.integer(Sys.getenv("KRIGR_CORES", "1"))
      )
    }, error = function(e) {
      warning(sprintf("  ! Falló descarga %s (%s): %s. Se salta esta combinación.", var, m$fun, e$message))
      return(NULL)
    })
    if (is.null(ras)) next
    
    terra::crs(ras) <- "EPSG:4326"
    
    # 1.2) Conversión de unidades (si hay regla definida)
    if (!is.null(unit_conversion_rules[[var]]) && is.function(unit_conversion_rules[[var]])) {
      ras <- unit_conversion_rules[[var]](ras)
    }
    
    # 1.3) Guardar raster (opcional pero útil para auditoría/reuso)
    terra::writeRaster(
      ras,
      filename = file.path(dir_tmp, sprintf("%s_%s_ca_diaria_%s.tif", var, m$fun, label)),
      datatype  = "FLT4S",
      overwrite = TRUE,
      NAflag    = -9999
    )
    
    # 1.4) Fechas asociadas a las bandas
    ras_dates <- seq.Date(as.Date(date_start_str), as.Date(date_stop_str), by = "day")
    if (terra::nlyr(ras) != length(ras_dates)) {
      ras_time <- tryCatch(terra::time(ras), error = function(e) NULL)
      if (!is.null(ras_time) && length(ras_time) == terra::nlyr(ras)) {
        ras_dates <- as.Date(ras_time)
      } else {
        warning("  ! Nº de bandas no coincide con fechas esperadas; se usará la secuencia DateStart/DateStop.")
      }
    }
    
    # 1.5) Incendios cuya ALARM_DAY cae dentro del rango
    in_range <- !is.na(combined_sf$ALARM_DAY) &
      (combined_sf$ALARM_DAY >= min(ras_dates)) &
      (combined_sf$ALARM_DAY <= max(ras_dates))
    
    u_days <- sort(unique(combined_sf$ALARM_DAY[in_range]))
    filled <- 0L
    
    # 1.6) Extraer por día para evitar relecturas
    for (d in u_days) {
      idx <- match(as.Date(d), ras_dates)
      if (is.na(idx)) next
      
      rows_d <- which(combined_sf$ALARM_DAY == d)
      if (!length(rows_d)) next
      
      vals_df <- terra::extract(ras[[idx]], pts[rows_d])
      vals <- vals_df[[2]]  # 1a col = ID interno; 2a = valor
      
      combined_sf[[colname]][rows_d] <- vals
      filled <- filled + sum(!is.na(vals))
    }
    
    cat(sprintf("  ✓ %s (%s): valores asignados para %d registros.\n", var, m$suffix, filled))
  }
}

cat("✓ Todas las combinaciones VARIABLE × AGGREGATE_FUNCTIONS asignadas.\n")
# ==== FIN DEL BLOQUE NUEVO ====


# Reemplazar código de causas del incendio por el texto correspondiente
cause_key <- c(
  "1"  = "Lightning",
  "2"  = "Equipment Use",
  "3"  = "Smoking",
  "4"  = "Campfire",
  "5"  = "Debris",
  "6"  = "Railroad",
  "7"  = "Arson",
  "8"  = "Playing with Fire",
  "9"  = "Miscellaneous",
  "10" = "Vehicle",
  "11" = "Powerline",
  "12" = "Firefighter Training",
  "13" = "Non-Firefighter Training",
  "14" = "Unknown/Unidentified",
  "15" = "Structure Fire",
  "16" = "Aircraft",
  "17" = "Volcanic",
  "18" = "Escaped Prescribed Burn",
  "19" = "Illegal Alien Campfire"
)

combined_sf <- 
  combined_sf |>
  mutate(
    CAUSE = factor(
      CAUSE,
      levels = names(cause_key),
      labels = cause_key
    )
  )

# Reemplazar código del método de recolección del perímetro
c_method_key <- c(
  "1" = "GPS Ground",
  "2" = "GPS Air",
  "3" = "Infrared (IR)",
  "4" = "Other Imagery",
  "5" = "Photo Interpretation",
  "6" = "Hand-drawn",
  "7" = "Mixed Methods",
  "8" = "Unknown"
)

combined_sf <- 
  combined_sf |>
  dplyr::mutate(
    C_METHOD = factor(
      C_METHOD,
      levels = names(c_method_key),
      labels = c_method_key
    )
  )

# Reempazar código del objetivo de manejo
objective_key <- c(
  "1" = "Suppression (Wildfire)",
  "2" = "Resource Benefit (Wildland Fire Use)"
)

combined_sf <- 
  combined_sf |>
  mutate(
    OBJECTIVE = factor(
      OBJECTIVE,
      levels = names(objective_key),
      labels = objective_key
    )
  )

# Reempazar código de la agencia responsable
agency_key <- c(
  "BIA" = "BIA Bureau of Indian Affairs",
  "BLM" = "BLM Bureau of Land Management",
  "CDF" = "CDF CAL FIRE (State)",
  "CCO" = "CCO Contract County",
  "DOD" = "DOD Department of Defense",
  "FWS" = "FWS U.S. Fish & Wildlife Service",
  "LRA" = "LRA Local Responsibility Area",
  "NOP" = "NOP No Protection",
  "NPS" = "NPS National Park Service",
  "OTH" = "OTH Other / Unknown Agency",
  "PVT" = "PVT Private Lands",
  "USF" = "USF U.S. Forest Service"
)

combined_sf <- 
  combined_sf |>
  mutate(
    AGENCY = factor(
      AGENCY,
      levels = names(agency_key),
      labels = agency_key
    )
  )


# Guardar archivos de salida
if (!is.null(combined_sf)) {
  cat("Guardando archivos de salida ...\n")
  # GeoJSON
  st_write(combined_sf, output_geojson, driver = "GeoJSON", delete_dsn = TRUE)
  cat("Datos en formato GeoJSON guardados exitosamente en:", output_geojson, "\n")
  
  # CSV (solo atributos, sin geometrías)
  write.csv(st_drop_geometry(combined_sf), output_csv, row.names = FALSE, fileEncoding = "UTF-8")
  cat("Datos en formato CSV guardados exitosamente en:", output_csv, "\n")
} else {
  cat("No se encontraron datos espaciales válidos en la respuesta.\n")
}
