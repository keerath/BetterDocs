#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script is a helper script to upload spark generated output to elasticSearch.
# DISCLAIMER: There are no defensive checks please use it carefully.

echo "Clearing kodebeagle related java indices from elasticsearch."
curl -XDELETE 'http://localhost:9200/java/'

# Updating mappings and types for kodebeagle index.
curl -XPOST localhost:9200/java/ -d '{
	"settings": {
		"index": {
			"number_of_shards": 3,
			"number_of_replicas": 1
		},
		"analysis": {
			"analyzer": {
				"ShingleAnalyzer": {
					"tokenizer": "standard",
					"filter": [
						"standard",
						"camel_filter",
						"lowercase",
						"filter_shingle"
					]
				},
				"camel": {
					"type": "pattern",
					"pattern": "([^\\p{L}\\d]+)|(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)|(?<=[\\p{L}&&[^\\p{Lu}]])(?=\\p{Lu})|(?<=\\p{Lu})(?=\\p{Lu}[\\p{L}&&[^\\p{Lu}]])"
				},
				"keyword_analyzer": {
					"type": "custom",
					"tokenizer": "keyword",
					"filter": ["lowercase"]
				}
			},
			"filter": {
				"filter_shingle": {
					"type": "shingle",
					"max_shingle_size": 5,
					"min_shingle_size": 2,
					"output_unigrams": true
				},
				"camel_filter": {
					"type": "word_delimiter",
					"generate_number_parts": false,
					"stem_english_possessive": false,
					"split_on_numerics": false
				}
			}
		}
	}
}'

curl -X PUT localhost:9200/java/typereference/_mapping -d '{
	"typereference": {
		"properties": {
			"file": {
				"type": "string",
				"index": "not_analyzed"
			},
			"payload": {
				"properties": {
					"types": {
						"properties": {
							"name": {
								"type": "string",
								"index": "no"
							},
							"props": {
								"properties": {
									"name": {
										"type": "string",
										"index": "no"
									},
									"lines": {
										"type": "long",
										"index": "no"
									}
								}
							}
						}
					},
					"score": {
						"type": "long",
						"index": "no"
					},
					"file": {
						"type": "string",
						"index": "no"
					}
				}
			},
			"contexts": {
				"properties": {
					"types": {
						"type": "nested",
						"include_in_parent": true,
						"properties": {
							"name": {
								"type": "string",
								"norms": { "enabled": false },
								"analyzer": "keyword_analyzer"
							},
							"props": {
								"type": "string",
								"norms": { "enabled": false },
								"analyzer": "keyword_analyzer"
							}
						}
					},
					"text": {
						"type": "string",
						"norms": { "enabled": false },
						"analyzer": "simple"
					}
				}
			}
		}
	}
}'

curl -X PUT localhost:9200/java/aggregation/_mapping -d '{
	"aggregation": {
		"properties": {
			"name": {
				"type": "string",
				"analyzer": "keyword_analyzer"
			},
			"score": {
				"type": "long"
			},
			"context": {
				"type": "string"
			},
			"vars": {
				"type": "nested",
				"properties": {
					"name": {
						"type": "string"
					},
					"count": {
						"type": "long"
					}
				}
			},
			"methods": {
				"type": "nested",
				"properties": {
					"name": {
						"type": "string"
					},
					"params": {
						"type": "long"
					},
					"count": {
						"type": "long"
					}
				}
			},
			"typeSuggest": {
				"type": "completion",
				"analyzer": "simple",
				"payloads": true
			},
			"methodSuggest": {
				"type": "completion",
				"analyzer": "simple",
				"payloads": true
			},
			"searchText": {
				"analyzer": "ShingleAnalyzer",
				"type": "string"
			}
		}
	}
}'

curl -XPUT localhost:9200/java/filemetadata/_mapping -d '{
  "filemetadata": {
    "properties": {
      "repoId": {
        "type": "integer",
        "index": "not_analyzed"
      },
      "fileName": {
        "type": "string",
        "index": "not_analyzed"
      },
      "superTypes": {
        "type": "object",
        "enabled": false
      },
      "fileTypes": {
        "properties": {
          "fileType": {
            "type": "string",
            "analyzer": "keyword_analyzer"
          },
          "loc": {
            "type": "string",
            "index": "no"
          }
        }
      },
      "externalRefList": {
        "type": "object",
        "enabled": false
      },
      "methodDefinitionList": {
        "type": "object",
        "enabled": false
      },
      "internalRefList": {
        "type": "object",
        "enabled": false
      }
    }
  }
}'

curl -XPUT localhost:9200/java/sourcefile/_mapping -d '{
	"sourcefile": {
		"properties": {
			"repoId": {
				"type": "integer",
				"index": "not_analyzed"
			},
			"fileName": {
				"type": "string",
				"index": "not_analyzed"
			},
			"fileContent": {
				"type": "string",
				"index": "no"
			}
		}
	}
}'

curl -XPUT localhost:9200/java/repotopic/_mapping -d '{
	"repotopic": {
		"properties": {
			"defaultBranch": {
				"type": "string"
			},
			"fork": {
				"type": "boolean"
			},
			"id": {
				"type": "long"
			},
			"language": {
				"type": "string"
			},
			"login": {
				"type": "string"
			},
			"repoName": {
				"type": "string"
			},
			"files": {
				"type": "object",
				"properties": {
					"file": {
						"type": "string"
					},
					"klscore": {
						"type": "double"
					}
				}
			},
			"topic": {
				"type": "object",
				"properties": {
					"term": {
						"type": "string"
					},
					"freq": {
						"type": "long"
					}
				}
			}
		}
	}
}'


curl -XPUT localhost:9200/java/documentation/_mapping -d '{
	"documentation": {
		"properties": {
			"fileName": {
				"type": "string",
				"index": "not_analyzed"
			},
			"docs": {
                "type": "nested",
                "include_in_parent": true,
                "properties": {
                    "typeName": {
                      "type": "string",
                      "analyzer": "keyword_analyzer"
                    },
                    "typeDoc": {
                      "type": "string",
                      "index": "no"
                    },
                    "propertyDocs": {
                       "properties": {
                         "propertyName": {
                           "type": "string",
                           "analyzer": "keyword_analyzer"
                         },
                         "propertyDoc": {
                           "type": "string",
                           "index": "no"
                         }
                       }

                    }
                }

            }
        }
	}
}'

curl -XPUT localhost:9200/java/filedetails/_mapping -d '{
  "filedetails": {
    "properties": {
      "file": {
        "type": "string",
        "index": "not_analyzed"
      },
      "commits": {
        "type": "object",
        "enabled": "false"
      },
      "topAuthors": {
        "type": "string",
        "index": "no"
      },
      "coChange": {
        "type": "string",
        "index": "no"
      }
    }
  }
}'
 

curl -XPUT localhost:9200/java/repodetails/_mapping -d '{
  "repodetails": {
    "properties": {
      "remote": {
        "type": "string",
        "index": "not_analyzed"
      },
      "gitHubInfo": {
        "properties": {
          "id": {
            "type": "long",
            "index": "not_analyzed"
          },
          "login": {
            "type": "string",
            "index": "not_analyzed"
          },
          "name": {
            "type": "string",
            "index": "not_analyzed"
          },
          "fullName": {
            "type": "string",
            "index": "not_analyzed"
          },
          "isPrivate": {
            "type": "boolean",
            "index": "not_analyzed"
          },
          "isFork": {
            "type": "boolean",
            "index": "not_analyzed"
          },
          "size": {
            "type": "long",
            "index": "not_analyzed"
          },
          "watchersCount": {
            "type": "long",
            "index": "not_analyzed"
          },
          "language": {
            "type": "string",
            "index": "not_analyzed"
          },
          "forksCount": {
            "type": "long",
            "index": "not_analyzed"
          },
          "subscribersCount": {
            "type": "long",
            "index": "not_analyzed"
          },
          "defaultBranch": {
            "type": "string",
            "index": "not_analyzed"
          },
          "stargazersCount": {
            "type": "long",
            "index": "not_analyzed"
          }
        }
      },
      "stats": {
      "properties": {
        "sloc": {
          "type": "long",
          "index": "not_analyzed"
        },
        "fileCount": {
          "type": "long",
          "index": "not_analyzed"
        },
        "size": {
          "type": "long",
          "index": "not_analyzed"
        }
      	}
      },
      "gitHistory": {
        "type": "object",
        "enabled": false
      }
    }
  }
}'




for f in `find $1 -name '*'`
do
    echo "uploading $f to elasticsearch."
    curl -s -XPOST 'localhost:9200/_bulk' --data-binary '@'$f >/dev/null
done
