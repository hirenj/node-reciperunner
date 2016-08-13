"use strict";

const fs = require('fs');
const path = require('path');
const glob = require("glob");
const snake = require('snake-case');
const keys    = require('map-tabular-keys');
const coerce  = require('coerce-tabular');
const tabular = require('tabular-stream');
const excel   = require('excel-stream');
const PassThrough = require('stream').PassThrough;
const RecipeRunner = require('./RecipeRunner');

const RecipeFolder = function(input_path) {
  this.path = input_path;
  this.ready = find_recipes(this.path).then((files) => this.recipes = files.map(read_recipe));
  this.recipe_streams = this.ready.then(() => run_recipes(this.recipes));
};

const find_recipes = function(input_path) {
  return new Promise(function(resolve,reject) {
    glob(path.join(input_path,'/**/*.recipe.json'), function (err, files) {
      if (err) {
        reject(err);
      } else {
        resolve(files);
      }
    });
  });
};

const excel_sheet = function(input,sheet_index,keyfunc) {
  return input.pipe(excel({sheetIndex: sheet_index, enclosedChar:'"'})).pipe(keys(keyfunc)).pipe(coerce());
};

const read_recipe = function(input_path) {
  let recipe = JSON.parse(fs.readFileSync(input_path));
  recipe.base = path.dirname(input_path);
  recipe.filename = path.basename(input_path);
  return recipe;
};

const write_recipe = function(stream,base,filename) {
  stream.pipe(fs.createWriteStream(path.join('output',filename)));
};

const read_file = function(filename) {
  return fs.createReadStream(filename);
}

const run_recipes = function(recipes) {
  let source_files = {};
  let runners = [];
  let inputs = [];
  recipes.forEach(function(recipe,idx) {
    recipe.sources.files.forEach(function(source) {
      let file_index = source.split(':');
      let sheet = null;
      if (file_index.length > 1) {
        sheet = file_index[1];
        source = file_index[0];
      }
      let abs_path = path.join(recipe.base,source);
      if (! source_files[abs_path]) {
        source_files[abs_path] = { 'recipes' : [], 'sheet' : sheet };
      }
      source_files[abs_path].recipes.push(idx);
    });
    let joiner = new PassThrough({objectMode : true});
    let runner = new RecipeRunner(recipe);
    joiner.pipe(runner);
    inputs.push(joiner);
    runners.push(runner);
    write_recipe(runner,recipe.base,recipe.filename);
  });

  Object.keys(source_files).map(function(filekey) {
    let stream = null;
    let fileinfo = source_files[filekey];
    let readstream = read_file(filekey);
    if (fileinfo.sheet) {
      stream = excel_sheet( readstream, parseInt(fileinfo.sheet),snake);
    } else {
      stream = readstream.pipe( tabular(snake) );
    }
    let count = 0;
    stream.setMaxListeners(fileinfo.recipes.length + 1);
    // stream.on('data',function(dat) {
    //   if (count++ >= 50) {
    //     fileinfo.recipes.forEach( (idx) => stream.unpipe(inputs[idx]));
    //     stream.removeAllListeners('data');
    //   } else {
    //     console.log(dat);
    //   }
    // });
    fileinfo.recipes.forEach( (idx) => stream.pipe(inputs[idx]));
  });

  return runners;
};

RecipeFolder.prototype.writeToPath = function(path) {

};

exports.RecipeFolder = RecipeFolder;