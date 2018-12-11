#!/usr/bin/env node

const fs = require('fs')
const { execSync } = require('child_process')
const process = require('process')
const opts = require('commander')

// default variables
const SOLR_URL = 'http://localhost:9983'

// global variables
let solrUrl
let targetDirectory

if (require.main === module) {
  opts.version('0.0.1')
    .option('-c, --core [value]', 'Core name')
    .option('-d, --dir [value]', 'Directory to load json dump files')
    .option('--solr [value]', 'Solr base url')
    .parse(process.argv)

  if (!(opts.core)) {
    console.error('\nMust provide a core name (-c) to dump data')
    opts.help()
  } else if (!(opts.dir)) {
    console.error('\nMust provide directory to load')
    opts.help()
  }

  solrUrl = (opts.solr) ? opts.solr : SOLR_URL
  targetDirectory = opts.dir || opts.core

  fs.access(targetDirectory, fs.constants.F_OK | fs.constants.R_OK, (err) => {
    if (err) {
      if (err.code === 'ENOENT') {
        console.error(`${targetDirectory} is not readable`)
      }
    } else {
      const files = fs.readdirSync(targetDirectory).sort()
      postFiles(files)
    }
  })
}

function postFiles (files) {
  const start = process.hrtime()
  for (let i = 0, len = files.length; i < len; i++) {
    const file = `${targetDirectory}/${files[i]}`
    console.log(`processing ${file} (${i + 1}/${len})`)
    // const { err } = await execSync(`ls ${file}`)
    // if (err) {
    //   console.error(`Error: ${err}`)
    // }
    // console.log(`${file} processed`)

    const { err } = execSync(`curl '${solrUrl}/solr/genome/update?commit=false' -X POST -H 'Content-Type: application/json' -d @${file} `)
    if (err) {
      console.error(err)
    }
    console.log(`${file} processed`)
  }
  const elapsed = process.hrtime(start)
  console.log(`Posting files completed in ${elapsed[0]} seconds`)
}
