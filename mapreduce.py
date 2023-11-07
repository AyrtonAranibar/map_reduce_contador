# Codigo hecho por Ayrton Aranibar Castillo
import threading

def eliminar_caracteres(texto, caracteres_a_eliminar):
    for caracter in caracteres_a_eliminar:
        texto = texto.replace(caracter, "")
    return texto

# Abre el archivo y realiza la limpieza
with open('DonQuijoteCap1.txt', 'r', encoding='utf-8') as archivo:
    contenido1 = archivo.read()

with open('DonQuijoteCap2.txt', 'r', encoding='utf-8') as archivo:
    contenido2 = archivo.read()

with open('DonQuijoteCap3.txt', 'r', encoding='utf-8') as archivo:
    contenido3 = archivo.read()

with open('DonQuijoteCap4.txt', 'r', encoding='utf-8') as archivo:
    contenido4 = archivo.read()

# Limpiamos el texto eliminando los siguientes caracteres
caracteres_a_eliminar = [",", ".", "(", ")", ";", "!", "¡", "-", "¿", "?", "«", "»", "'", ":"]

contenido1 = eliminar_caracteres(contenido1, caracteres_a_eliminar).lower()
contenido2 = eliminar_caracteres(contenido2, caracteres_a_eliminar).lower()
contenido3 = eliminar_caracteres(contenido3, caracteres_a_eliminar).lower()
contenido4 = eliminar_caracteres(contenido4, caracteres_a_eliminar).lower()

contenido_total = contenido1 + " " + contenido2 + " " + contenido3 + " " + contenido4

# Guardamos el archivo para testear el resultado
with open('DonQuijoteLimpio.txt', 'w', encoding='utf-8') as archivo_salida:
    archivo_salida.write(contenido_total)
    
#print(contenido1)

def mapeo(archivo, mapa):
    # Este split no es el mismo que el de map reduce
    # Ese split divide el texto en grupos de datos manejables de tamaño especifico
    archivo = archivo.split()
    for clave in archivo:
        mapa.append((clave,[1]))

# Sincroniza los hilos para que no hayan interferencias y por lo tanto errores
lock = threading.Lock()

# Array para que nos permita agregar mas de un elemento con las misma clave
mapeo1 = []
mapeo2 = []
mapeo3 = []
mapeo4 = []

# Crear hilos para procesar el contenido
# // NODO 1 //
hilo1 = threading.Thread(target=mapeo, args=(contenido1, mapeo1))
hilo2 = threading.Thread(target=mapeo, args=(contenido2, mapeo2))
# // NODO 2 //
hilo3 = threading.Thread(target=mapeo, args=(contenido3, mapeo3))
hilo4 = threading.Thread(target=mapeo, args=(contenido4, mapeo4))

# Iniciar los hilos
hilo1.start()
hilo2.start()
hilo3.start()
hilo4.start()

# Esperar a que los hilos terminen
hilo1.join()
hilo2.join()
hilo3.join()
hilo4.join()

#print("/////////////////////// MAPEO DEL HILO 1 //////////////////////////")
#print(mapeo1)
#print("/////////////////////// MAPEO DEL HILO 2 //////////////////////////")
#print(mapeo2)
#print("/////////////////////// MAPEO DEL HILO 3 //////////////////////////")
#print(mapeo3)
#print("/////////////////////// MAPEO DEL HILO 4 //////////////////////////")
#print(mapeo4)

def ordenamiento(mapa, resultado):
    resultado.extend(sorted(mapa, key=lambda x: x[0]))

ordenamiento1 = []
ordenamiento2 = []
ordenamiento3 = []
ordenamiento4 = []

# // NODO 1 //
hilo1 = threading.Thread(target=ordenamiento, args=(mapeo1, ordenamiento1))
hilo2 = threading.Thread(target=ordenamiento, args=(mapeo2, ordenamiento2))
# // NODO 2 //
hilo3 = threading.Thread(target=ordenamiento, args=(mapeo3, ordenamiento3))
hilo4 = threading.Thread(target=ordenamiento, args=(mapeo4, ordenamiento4))

# Iniciar los hilos
hilo1.start()
hilo2.start()
hilo3.start()
hilo4.start()

# Esperar a que los hilos terminen
hilo1.join()
hilo2.join()
hilo3.join()
hilo4.join()

#print(ordenamiento1)

mapa_global = [ordenamiento1,ordenamiento2,ordenamiento3,ordenamiento4]

# Primero junta todas las claves con sus valores, y de alli los separa en 2 para hacer reduce



def baraja(mapa_global):
    baraja1 = {}
    baraja2 = {}
    for mapa in mapa_global:
        for clave, valor in mapa:
            # Ordenamiento y Merge
            # si la primera letra de la clave esta entre la a y la n
            if 'a' <= clave[0] <= 'n':
                if clave in baraja1:
                    # Merge
                    baraja1[clave].extend(valor) 
                else:
                    baraja1[clave] = valor 
            else:
                if clave in baraja2:
                    # Merge
                    baraja2[clave].extend(valor) 
                else:
                    baraja2[clave] = valor
    baraja1 = dict(sorted(baraja1.items(), key=lambda item: item[0])) 
    baraja2 = dict(sorted(baraja2.items(), key=lambda item: item[0]))  
    return baraja1,baraja2

baraja1, baraja2 = baraja(mapa_global)
#print(baraja1)

# Crear una lista para almacenar los resultados
resultados =  {}

def reducir(mapa, resultados):
    nuevo_mapa = {}
    for palabra, valores in mapa.items():
        suma_valores = 0
        for valor in valores:
            suma_valores += valor
        nuevo_mapa[palabra] = suma_valores
    resultados[threading.current_thread().name] = nuevo_mapa





# Crear instancias de hilos para realizar la reducción en paralelo
hilo1 = threading.Thread(target=reducir, args=(baraja1, resultados))
hilo2 = threading.Thread(target=reducir, args=(baraja2, resultados))
# Iniciar los hilos
hilo1.start()
hilo2.start()

# Esperar a que los hilos terminen
hilo1.join()
hilo2.join()

resultado_total = {}

# Iterar a través de los hilos y combinar sus resultados en "resultado_total".
for thread_name, thread_result in resultados.items():
    resultado_total.update(thread_result)

print(resultado_total)

# Ahora, "resultado_total" contendrá todos los elementos de los hilos combinados en un solo diccionario.
# Ordenamiento final
#resultado_total = dict(sorted(resultado_total.items(), key=lambda item: item[0]))

# Guardo el documento con un formato de diccionario para aprovechar el hash
cadena_diccionario = '{\n'
for clave, valor in resultado_total.items():
    cadena_diccionario += f"    '{clave}': {valor},\n"
cadena_diccionario += '}'

# Almacenamos el resultado en un documento 
with open('diccionario_conteo.txt', 'w') as archivo:
    archivo.write(cadena_diccionario)

