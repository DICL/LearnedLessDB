#include "koo/koo.h"
#include "koo/util.h"

using std::to_string;

namespace koo {
bool run_sosd = false;
std::string sosd_data_path = "";
std::string sosd_lookups_path = "";

std::string model_dbname = "/models";
double learn_model_error = 8;
double merge_model_error = 21;
uint64_t min_num_keys = 30;

}
