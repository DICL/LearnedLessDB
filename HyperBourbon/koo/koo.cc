#include "koo/koo.h"
#include "koo/util.h"

using std::to_string;

namespace koo {

bool run_sosd = false;
std::string sosd_data_path = "";
std::string sosd_lookups_path = "";

std::string model_dbname = "/models";
double learn_model_error = 8;
int mod = 0;

std::condition_variable cv;
std::mutex cv_mtx;
std::atomic<bool> should_stop{false};

}
