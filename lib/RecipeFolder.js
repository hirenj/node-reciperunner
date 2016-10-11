'use strict';

const fs = require('fs');
const path = require('path');
const glob = require('glob');
const snake = require('snake-case');
const keys    = require('map-tabular-keys');
const coerce  = require('coerce-tabular');
const tabular = require('tabular-stream');
const excel   = require('excel-stream');
const PassThrough = require('stream').PassThrough;
const RecipeRunner = require('./RecipeRunner');

const ConcatStream = require('stream-concat');

const RecipeFolder = function(input_path,options) {
  if ( ! options ) {
    options = {};
  }
  this.path = input_path;
  this.output = options.output;
  this.ready = this.findRecipes(this.path)
                   .then((files) => Promise.all(files.map(this.readRecipe.bind(this))) )
                   .then((recipes) => this.recipes = recipes );
  this.recipe_streams = this.ready
                        .then(() => Promise.all(this.recipes.map((recipe) => this.needsUpdate(recipe) )))
                        .then(() => this.runRecipes(this.recipes,options.debug));
};

RecipeFolder.prototype.findRecipes = function(input_path) {
  let paths = input_path.split(',');
  if (paths.length > 1) {
    return Promise.all(paths.map((a_path) => this.findRecipes(a_path))).then( (paths_arr) => [].concat.apply([], paths_arr ) );
  }
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

RecipeFolder.prototype.readRecipe = function(input_path) {
  let recipe = JSON.parse(fs.readFileSync(input_path));
  recipe.base = path.dirname(input_path);
  recipe.filename = path.basename(input_path);
  return Promise.resolve(recipe);
};

RecipeFolder.prototype.writeRecipe = function(stream,recipe) {
  if (this.output) {
    try {
      fs.mkdirSync(this.output);
    } catch(err) {
      if (err.code == 'EEXIST') {
      } else {
        throw err;
      }
    }
  }
  let target_filename = snake(path.basename(recipe.base)+'_'+recipe.filename.replace('recipe.json',''));
  let outstream = fs.createWriteStream(path.join((this.output || ''),target_filename+'.json'));
  stream.pipe(outstream);
  return new Promise(function(resolve,reject){
    outstream.on('close',resolve);
    outstream.on('error',reject);
  });
};

RecipeFolder.prototype.readFile = function(filename) {
  return fs.createReadStream(filename);
};

RecipeFolder.prototype.needsUpdate = function(recipe) {
  recipe.needs_update = true;
  return true;
};

RecipeFolder.prototype.runRecipes = function(recipes,debug) {
  let self = this;
  let source_files = {};
  let runners = [];
  let inputs = [];
  let written_promises = [];

  recipes.forEach(function(recipe,idx) {
    if ( ! recipe.needs_update ) {
      return;
    }
    recipe.sources.files.forEach(function(source) {
      let file_index = source.split(':');
      let sheet = null;
      if (file_index.length > 1) {
        sheet = file_index[1];
        source = file_index[0];
      }
      let abs_path = path.join(recipe.base,source);
      if (! source_files[abs_path]) {
        source_files[abs_path] = { 'recipes' : [], 'sheets' : [] };
      }
      source_files[abs_path].recipes.push(idx);
      if (sheet && source_files[abs_path].sheets.indexOf(sheet) < 0) {
        source_files[abs_path].sheets.push(sheet);
      }
    });
    let joiner = new PassThrough({objectMode : true});
    let runner = new RecipeRunner(recipe);
    joiner.pipe(runner);
    inputs.push(joiner);
    runners.push(runner);

    written_promises.push(self.writeRecipe(runner,recipe));
  });

  this.written = Promise.all(written_promises);

  Object.keys(source_files).map(function(filekey) {
    let stream = null;
    let fileinfo = source_files[filekey];

    let readstream = self.readFile(filekey);
    if (fileinfo.sheets && fileinfo.sheets.length > 0) {
      if (debug) {
        console.log(filekey,'Extracting from single Excel sheet '+fileinfo.sheets[0]);
        stream = excel_sheet( readstream, parseInt(fileinfo.sheets[0]),snake);
      } else {
        console.log(filekey,'Extracting from multiple excel sheets '+fileinfo.sheets.join(','));
        let streams = fileinfo.sheets
                         .map( (sheetidx) => excel_sheet( readstream, parseInt(sheetidx),snake)  );
        stream = new ConcatStream(streams,{objectMode: true});
      }
    } else {
      stream = readstream.pipe( tabular(snake) );
    }
    let count = 0;
    stream.setMaxListeners(2*fileinfo.recipes.length + 4);
    if (debug && ! isNaN(debug)) {
      stream.on('data',function(dat) {
        if (count++ >= +debug) {
          fileinfo.recipes.forEach( (idx) => stream.unpipe(inputs[idx]));
          stream.removeAllListeners('data');
        } else {
          process.stderr.write(JSON.stringify(dat)+'\n');
        }
      });
    }
    fileinfo.recipes.forEach( (idx) => stream.pipe(inputs[idx]));
  });

  return runners;
};

exports.RecipeFolder = RecipeFolder;