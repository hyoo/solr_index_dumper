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
  const fieldListHash = {
    'genome': 'taxon_lineage_ids,collection_date,collection_year,habitat,genome_id,genome_name,other_typing,body_sample_site,contigs,patric_cds,publication,genome_status,isolation_site,temperature_range,isolation_country,common_name,order,longitude,strain,chromosomes,biovar,biosample_accession,isolation_comments,cell_shape,p2_genome_id,genbank_accessions,culture_collection,refseq_accessions,genus,antimicrobial_resistance_evidence,organism_name,additional_metadata,altitude,sequencing_platform,host_gender,latitude,refseq_cds,other_clinical,sra_accession,body_sample_subsite,genome_length,public,owner,user_read,user_write,reference_genome,oxygen_requirement,taxon_lineage_names,gram_stain,gc_content,antimicrobial_resistance,class,pathovar,sporulation,ncbi_project_id,owner,sequencing_depth,salinity,optimal_temperature,comments,disease,geographic_location,taxon_id,plasmids,kingdom,assembly_method,sequencing_centers,host_age,phylum,depth,mlst,species,assembly_accession,host_health,serovar,motility,refseq_project_id,type_strain,completion_date,sequencing_status,family,bioproject_accession,host_name,isolation_source,date_inserted,date_modified',
    'genome_feature': 'feature_id,genome_id,na_length,genome_name,alt_locus_tag,p2_feature_id,aa_sequence_md5,accession,segments,strand,public,property,sequence_id,refseq_locus_tag,end,aa_length,annotation,owner,product,na_sequence_md5,gene,start,pos_group,go,taxon_id,patric_id,feature_type,protein_id,figfam_id,plfam_id,pgfam_id,location,gene_id,date_inserted,date_modified',
    'feature_sequence': 'md5,sequence_type,sequence,date_inserted,date_modified'
  }
  const fl = fieldListHash[coreName] || '*'
  const url = `${solrUrl}/solr/${coreName}/select?q=*:*&sort=${opts.uniqueKey}+asc&rows=${fetchSize}&start=0&wt=json&fl=${fl}&cursorMark=${encodeURIComponent(cursorMark)}`
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
