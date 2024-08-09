# mauiPackage
#
# This package has three main functions:
# Input data from the server
# Output data to arrow, converting the files from the server to parquet files
# Input the data in from arrow, with the new parquet files

fFetchMauiTrades <- function(){
  fInsFromServer <-  function(){
    cat(paste0('fetchMauiTrades.R::fInsFromServer Running...  ', Sys.time(), '\n'))
    fInsFile1 <- function(){
      cat(paste0('fetchMauiTrades.R::fInsFile1 Running...  ', Sys.time(), '\n'))
      sFile1 <- '/Volumes/data/tmp/interns/sprint_2024q3/trade_analysis_2024q3.txt'
      tbbl1 <- read.delim(sFile1, sep = "|", header = TRUE) %>% as_tibble()
      cat(paste0('fetchMauiTrades.R::fInsFile1 Complete.  ', Sys.time(), '\n'))
      #browser()
      return(tbbl1)
    }
    fInsFile2 <- function(){
      cat(paste0('fetchMauiTrades.R::fInsFile2 Running...  ', Sys.time(), '\n'))
      sFile2 <- '/Volumes/data/tmp/interns/sprint_2024q3/DAILY_S_SQL_QRY_HOLDINGS_TO_SALESFORCE.txt'
      tbbl2 <- read.delim(sFile2, sep = "|", header = TRUE) %>% as_tibble()
      cat(paste0('fetchMauiTrades.R::fInsFile2 Complete.  ', Sys.time(), '\n'))
      #browser()
      return(tbbl2)
    }
    fInsFile3 <- function(){
      cat(paste0('fetchMauiTrades.R::fInsFile3 Running...  ', Sys.time(), '\n'))
      sFile3 <- '/Volumes/data/tmp/interns/sprint_2024q3/DAILY_S_SQL_TTC_VIEW_ASSET.txt'
      tbbl3 <- read.delim(sFile3, sep = "|", header = TRUE) %>% as_tibble()
      cat(paste0('fetchMauiTrades.R::fInsFile3 Complete.  ', Sys.time(), '\n'))
      #browser()
      return(tbbl3)
    }
    fInsFile4 <- function(){
      cat(paste0('fetchMauiTrades.R::fInsFile4 Running...  ', Sys.time(), '\n'))
      sFile4 <- '/Volumes/data/tmp/interns/sprint_2024q3/DAILY_S_SQL_TTC_VIEW_OFFICER_ID.txt'
      tbbl4 <- read.delim(sFile4, sep = "|", header = TRUE) %>% as_tibble()
      cat(paste0('fetchMauiTrades.R::fInsFile4 Complete.  ', Sys.time(), '\n'))
      #browser()
      return(tbbl4)
    }

    cat(paste0('fetchMauiTrades.R::fInsFromServer Complete.  ', Sys.time(), '\n'))

    cat(paste0("FetchMauiTrades.R::CreateTribble Running ...  ", Sys.time(), '\n'))
    x <- tribble( ~source, ~myData
                  ,'tradeAnalysis.txt', fInsFile1()
                  ,'Salesforce.txt', fInsFile2()
                  ,'asset.txt', fInsFile3()
                  ,'OfficerID.txt', fInsFile4()
    )
    cat(paste0("FetchMauiTrades.R::CreateTribble Complete.  ", Sys.time(), '\n'))
    #browser()
    return(x)
  }

  fOutsToArrow <- function(){
    cat(paste0('fetchMauiTrades.R::fOutsToArrow Running...  ', Sys.time(), '\n'))

    fFile1ToArrow <- function(){
      cat(paste0('fetchMauiTrades.R::fFile1ToArrow Running...  ', Sys.time(), '\n'))
      sFile1 <- '/Volumes/data/tmp/interns/sprint_2024q3/trade_analysis_2024q3.txt'
      tbbl1 <- read.delim(sFile1, sep = "|", header = TRUE) %>% as_tibble()

      parquet_file1 <- '/Volumes/data/tmp/interns/sprint_2024q3/parquet/trade_analysis_2024q3.parquet'
      arrow_table1 <- as_arrow_table(tbbl1)
      write_parquet(arrow_table1, parquet_file1)

      cat(paste0('fetchMauiTrades.R::fFile1ToArrow Complete.  ', Sys.time(), '\n'))
    }

    fFile2ToArrow <- function(){
      cat(paste0('fetchMauiTrades.R::fFile2ToArrow Running...  ', Sys.time(), '\n'))
      sFile2 <- '/Volumes/data/tmp/interns/sprint_2024q3/DAILY_S_SQL_QRY_HOLDINGS_TO_SALESFORCE.txt'
      tbbl2 <- read.delim(sFile2, sep = "|", header = TRUE) %>% as_tibble()

      parquet_file2 <- '/Volumes/data/tmp/interns/sprint_2024q3/parquet/DAILY_S_SQL_QRY_HOLDINGS_TO_SALESFORCE.parquet'
      arrow_table2 <- as_arrow_table(tbbl2)
      write_parquet(arrow_table2, parquet_file2)

      cat(paste0('fetchMauiTrades.R::fFile2ToArrow Complete.  ', Sys.time(), '\n'))
    }

    fFile3ToArrow <- function(){
      cat(paste0('fetchMauiTrades.R::fFile3ToArrow Running...  ', Sys.time(), '\n'))
      sFile3 <- '/Volumes/data/tmp/interns/sprint_2024q3/DAILY_S_SQL_TTC_VIEW_ASSET.txt'
      tbbl3 <- read.delim(sFile3, sep = "|", header = TRUE) %>% as_tibble()

      parquet_file3 <- '/Volumes/data/tmp/interns/sprint_2024q3/parquet/DAILY_S_SQL_TTC_VIEW_ASSET.parquet'
      arrow_table3 <- as_arrow_table(tbbl3)
      write_parquet(arrow_table3, parquet_file3)

      cat(paste0('fetchMauiTrades.R::fFile3ToArrow Complete.  ', Sys.time(), '\n'))
    }

    fFile4ToArrow <- function(){
      cat(paste0('fetchMauiTrades.R::fFile4ToArrow Running...  ', Sys.time(), '\n'))
      sFile4 <- '/Volumes/data/tmp/interns/sprint_2024q3/DAILY_S_SQL_TTC_VIEW_OFFICER_ID.txt'
      tbbl4 <- read.delim(sFile4, sep = "|", header = TRUE) %>% as_tibble()

      parquet_file4 <- '/Volumes/data/tmp/interns/sprint_2024q3/parquet/DAILY_S_SQL_TTC_VIEW_OFFICER_ID.parquet'
      arrow_table4 <- as_arrow_table(tbbl4)
      write_parquet(arrow_table4, parquet_file4)

      cat(paste0('fetchMauiTrades.R::fFile4ToArrow Complete.  ', Sys.time(), '\n'))
    }

    fFile1ToArrow()
    fFile2ToArrow()
    fFile3ToArrow()
    fFile4ToArrow()

    cat(paste0('fetchMauiTrades.R::fOutsToArrow Complete.  ', Sys.time(), '\n'))
  }

  fInsFromArrow <- function(){
    cat(paste0('fetchMauiTrades.R::fInsFromArrow Running...  ', Sys.time(), '\n'))

    fFile1FromArrow <- function(){
      cat(paste0('fetchMauiTrades.R::fFile1FromArrow Running...  ', Sys.time(), '\n'))
      parquet_file1 <- '/Volumes/data/tmp/interns/sprint_2024q3/parquet/trade_analysis_2024q3.parquet'
      arrow_table1 <- read_parquet(parquet_file1)
      tbbl1 <- as_tibble(arrow_table1)

      cat(paste0('fetchMauiTrades.R::fFile1FromArrow Complete.  ', Sys.time(), '\n'))
      return(tbbl1)
    }

    fFile2FromArrow <- function(){
      cat(paste0('fetchMauiTrades.R::fFile2FromArrow Running...  ', Sys.time(), '\n'))
      parquet_file2 <- '/Volumes/data/tmp/interns/sprint_2024q3/parquet/DAILY_S_SQL_QRY_HOLDINGS_TO_SALESFORCE.parquet'
      arrow_table2 <- read_parquet(parquet_file2)
      tbbl2 <- as_tibble(arrow_table2)

      cat(paste0('fetchMauiTrades.R::fFile2FromArrow Complete.  ', Sys.time(), '\n'))
      return(tbbl2)
    }

    fFile3FromArrow <- function(){
      cat(paste0('fetchMauiTrades.R::fFile3FromArrow Running...  ', Sys.time(), '\n'))
      parquet_file3 <- '/Volumes/data/tmp/interns/sprint_2024q3/parquet/DAILY_S_SQL_TTC_VIEW_ASSET.parquet'
      arrow_table3 <- read_parquet(parquet_file3)
      tbbl3 <- as_tibble(arrow_table3)

      cat(paste0('fetchMauiTrades.R::fFile3FromArrow Complete.  ', Sys.time(), '\n'))
      return(tbbl3)
    }

    fFile4FromArrow <- function(){
      cat(paste0('fetchMauiTrades.R::fFile4FromArrow Running...  ', Sys.time(), '\n'))
      parquet_file4 <- '/Volumes/data/tmp/interns/sprint_2024q3/parquet/DAILY_S_SQL_TTC_VIEW_OFFICER_ID.parquet'
      arrow_table4 <- read_parquet(parquet_file4)
      tbbl4 <- as_tibble(arrow_table4)

      cat(paste0('fetchMauiTrades.R::fFile4FromArrow Complete.  ', Sys.time(), '\n'))
      return(tbbl4)
    }

    tbbl1 <- fFile1FromArrow()
    tbbl2 <- fFile2FromArrow()
    tbbl3 <- fFile3FromArrow()
    tbbl4 <- fFile4FromArrow()
    #browser()
    cat(paste0('fetchMauiTrades.R::fInsFromArrow Complete.  ', Sys.time(), '\n'))
  }

  fInsFromServer()
  fOutsToArrow()
  fInsFromArrow()

}

cat(paste0('FetchMauiTrades.R:: Running...', Sys.time(), '\n'))
cat(paste0('=======================================', '\n'))
fFetchMauiTrades()
cat(paste0('========================================', '\n'))
cat(paste0('FetchMauiTrades.R:: Complete. ', Sys.time(), '\n'))
