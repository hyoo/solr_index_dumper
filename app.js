#!/usr/bin/env node

const fs = require('fs')
const rp = require('request-promise')
const process = require('process')
const opts = require('commander')

// default variables
const SOLR_URL = 'http://localhost:9983'
const FETCH_SIZE = 10000
const CURSOR_MARK = '*'
const SERIAL_NUMBER_PADDING_SIZE = 5

// global variables
let totalFetched = 0
let solrUrl
let fetchSize
let targetDirectory

function fetchStreaming (coreName, serialNumber, cursorMark) {
  const url = `${solrUrl}/solr/${coreName}/select?q=*:*&sort=${opts.uniqueKey}+asc&rows=${fetchSize}&start=0&wt=json&cursorMark=${encodeURIComponent(cursorMark)}`
  console.log(`fetchStreaming [${coreName}, ${serialNumber}] [${totalFetched}]: ${url}`)

  rp.get(url, {
    json: true,
    headers: {
      'content-type': 'application/json'
    }
  })
    .then(body => {
      totalFetched += body.response.docs.length

      // save
      saveToFile(opts.core, targetDirectory, serialNumber, body.response.docs)

      if (body.nextCursorMark && body.response.numFound > totalFetched) {
        // fetch next
        fetchStreaming(opts.core, serialNumber + 1, body.nextCursorMark)
      } else {
        console.log(`Complete fetching ${totalFetched} records from ${coreName}`)
      }
    })
}

function saveToFile (prefix = 'file', dir, serialNumber, data) {
  const paddedSerialNumber = String.prototype.padStart.apply(serialNumber, [SERIAL_NUMBER_PADDING_SIZE, '0'])
  fs.writeFileSync(`./${dir}/${prefix}.${paddedSerialNumber}.json`, JSON.stringify(data), {encoding: 'utf8'})
}

if (require.main === module) {
  opts.version('0.0.1')
    .option('-c, --core [value]', 'Core name')
    .option('--uniqueKey [value]', 'uniqueKey field name')
    .option('-t, --target [value]', 'Directory to dump data')
    .option('--solr [value]', 'Solr base url')
    .option('--fetchSize <n>', 'Fetch Size')
    .parse(process.argv)

  if (!(opts.core)) {
    console.error('\nMust provide a core name (-c) to dump data')
    opts.help()
  } else if (!opts.uniqueKey) {
    console.error('\nMust provide unique key field name of the core')
    opts.help()
  }

  // global variables
  solrUrl = (opts.solr) ? opts.solr : SOLR_URL
  fetchSize = (opts.fetchSize) ? opts.fetchSize : FETCH_SIZE
  targetDirectory = opts.target || opts.core

  // check data directory
  fs.access(targetDirectory, fs.constants.F_OK | fs.constants.W_OK, (err) => {
    if (err) {
      if (err.code === 'ENOENT') {
        // does not exist. let's create one
        fs.mkdirSync(targetDirectory)
        // start fetching
        fetchStreaming(opts.core, 0, CURSOR_MARK)
      } else {
        console.error(`${targetDirectory} is read-only`)
      }
    } else {
      // targetDirectory exists, and it is writable
      // start fetching
      fetchStreaming(opts.core, 0, CURSOR_MARK)
    }
  })
}
