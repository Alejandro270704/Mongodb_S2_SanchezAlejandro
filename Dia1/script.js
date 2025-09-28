
use Victima;

db.createCollection("Departamento");
db.createCollection("Municipio");
db.createCollection("Sitio");
db.createCollection("Evento");
db.createCollection("Area");
db.createCollection("Estado");
db.createCollection("Genero");
db.createCollection("Tiempo");
db.createCollection("RangoEdad");
db.createCollection("GrupoEtnico");
db.createCollection("Condicion");

db.createCollection("Victima");



db.Departamento.insertMany([
    { _id: 1, NombreDepartamento: "ANTIOQUIA", CodigoDaneDepartamento: 5 },
    { _id: 2, NombreDepartamento: "CAQUETA", CodigoDaneDepartamento: 18 },
    { _id: 3, NombreDepartamento: "BOLIVAR", CodigoDaneDepartamento: 13 },
    { _id: 4, NombreDepartamento: "PUTUMAYO", CodigoDaneDepartamento: 86 },
    { _id: 5, NombreDepartamento: "CORDOBA", CodigoDaneDepartamento: 23 },
]);

db.Municipio.insertMany([
    { _id: 1, NombreMunicipio: "GRANADA", CodigoDaneMunicipio: 5313, IdDepartamento: 1 },
    { _id: 2, NombreMunicipio: "MONTAÑITA", CodigoDaneMunicipio: 18410, IdDepartamento: 2 },
    { _id: 3, NombreMunicipio: "ARENAL", CodigoDaneMunicipio: 13042, IdDepartamento: 3 },
    { _id: 4, NombreMunicipio: "PUERTO ASÍS", CodigoDaneMunicipio: 86568, IdDepartamento: 4 },
    { _id: 5, NombreMunicipio: "PUERTO LIBERTADOR", CodigoDaneMunicipio: 23580, IdDepartamento: 5 }
]);

db.Sitio.insertMany([
    { _id: 1, NombreSitio: "Sin información" },
  { _id: 2, NombreSitio: "VEREDA QUEJANBI" },
  { _id: 3, NombreSitio: "VEREDAS LAS MERCEDES" },
  { _id: 4, NombreSitio: "VEREDA ADUANA" },
  { _id: 5, NombreSitio: "Vereda Doradas Altas" }
]);

db.Evento.insertMany([
     { _id: 1, NombreEvento: "Masacre", IdSitio: 1 },
    { _id: 2, NombreEvento: "Desplazamiento forzado", IdSitio: 2 },
    { _id: 3, NombreEvento: "Secuestro", IdSitio: 3 },
    { _id: 4, NombreEvento: "Reclutamiento forzado", IdSitio: 4 },
        { _id: 5, NombreEvento: "Atentado terrorista", IdSitio: 5 }
]);

db.Area.insertMany([
    { _id: 1, TipoArea: "Rural" },
    { _id: 2, TipoArea: "Urbana" }
]);

db.Estado.insertMany([
    { _id: 1, Estado: "Herido" },
    { _id: 2, Estado: "Muerto" }
]);

db.Genero.insertMany([
    { _id: 1, Genero: "Hombre" },
    { _id: 2, Genero: "Mujer" }
]);

db.Tiempo.insertMany([
    { _id: 1, Ano: 2006, Mes: "1" },
  { _id: 2, Ano: 2009, Mes: "12" },
  { _id: 3, Ano: 2023, Mes: "2" },
  { _id: 4, Ano: 2008, Mes: "4" },
  { _id: 5, Ano: 2009, Mes: "11" },
]);

db.RangoEdad.insertMany([
    { _id: 1, RangoEdad: "mayor de 18 años" },
  { _id: 2, RangoEdad: "menor de 18 años" }
]);

db.GrupoEtnico.insertMany([
    { _id: 1, GrupoEtnico: "No aplica" },
  { _id: 2, GrupoEtnico: "Indigena" },
  { _id: 3, GrupoEtnico: "Afrodescendiente" }
]);

db.Condicion.insertMany([
    { _id: 1, Condicion: "Civil" },
  { _id: 2, Condicion: "Fuerza pública" }
]);


db.Victima.insertMany([
    {
        _id: 1,
        IdDepartamento: 1,
        IdMunicipio: 1,
        IdSitio: 1,
        IdEvento: 1,
        IdArea: 1,
        IdEstado: 1,
        IdGenero: 1,
        IdTiempo: 1,
        IdRangoEdad: 1,
        IdGrupoEtnico: 1,
        IdCondicion: 1
    },
    {
        _id: 2,
        IdDepartamento: 2,
        IdMunicipio: 2,
        IdSitio: 2,
        IdEvento: 2,
        IdArea: 2,
        IdEstado: 2,
        IdGenero: 2,
        IdTiempo: 2,
        IdRangoEdad: 2,
        IdGrupoEtnico: 2,
        IdCondicion: 2
    },
    {
        _id: 3,
        IdDepartamento: 3,
        IdMunicipio: 3,
        IdSitio: 3,
        IdEvento: 3,
        IdArea: 1,
        IdEstado: 2,
        IdGenero: 1,
        IdTiempo: 3,
        IdRangoEdad: 1,
        IdGrupoEtnico: 3,
        IdCondicion: 1
    },
    {
        _id: 4,
        IdDepartamento: 4,
        IdMunicipio: 4,
        IdSitio: 4,
        IdEvento: 4,
        IdArea: 2,
        IdEstado: 1,
        IdGenero: 2,
        IdTiempo: 4,
        IdRangoEdad: 2,
        IdGrupoEtnico: 1,
        IdCondicion: 2
    },
    {
        _id: 5,
        IdDepartamento: 5,
        IdMunicipio: 5,
        IdSitio: 5,
        IdEvento: 5,
        IdArea: 1,
        IdEstado: 2,
        IdGenero: 1,
        IdTiempo: 5,
        IdRangoEdad: 1,
        IdGrupoEtnico: 2,
        IdCondicion: 1
    }
]);



db.Victima.aggregate([
  { $lookup: { from: "Departamento", localField: "IdDepartamento", foreignField: "_id", as: "Departamento" } },
  { $lookup: { from: "Municipio", localField: "IdMunicipio", foreignField: "_id", as: "Municipio" } },
  { $lookup: { from: "Sitio", localField: "IdSitio", foreignField: "_id", as: "Sitio" } },
  { $lookup: { from: "Evento", localField: "IdEvento", foreignField: "_id", as: "Evento" } },
  { $lookup: { from: "Area", localField: "IdArea", foreignField: "_id", as: "Area" } },
  { $lookup: { from: "Estado", localField: "IdEstado", foreignField: "_id", as: "Estado" } },
  { $lookup: { from: "Genero", localField: "IdGenero", foreignField: "_id", as: "Genero" } },
  { $lookup: { from: "Tiempo", localField: "IdTiempo", foreignField: "_id", as: "Tiempo" } },
  { $lookup: { from: "RangoEdad", localField: "IdRangoEdad", foreignField: "_id", as: "RangoEdad" } },
  { $lookup: { from: "GrupoEtnico", localField: "IdGrupoEtnico", foreignField: "_id", as: "GrupoEtnico" } },
  { $lookup: { from: "Condicion", localField: "IdCondicion", foreignField: "_id", as: "Condicion" } },
  {
    $project: {
      _id: 1,  
      "Departamento.NombreDepartamento": 1,
      "Municipio.NombreMunicipio": 1,
      "Sitio.NombreSitio": 1,
      "Evento.NombreEvento": 1,
      "Area.TipoArea": 1,
      "Estado.Estado": 1,
      "Genero.Genero": 1,
      "Tiempo.Ano": 1,
      "Tiempo.Mes": 1,
      "RangoEdad.RangoEdad": 1,
      "GrupoEtnico.GrupoEtnico": 1,
      "Condicion.Condicion": 1
    }
  }
]).pretty();