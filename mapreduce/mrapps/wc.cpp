#include "worker.h"
#include <string>
#include <functional>
using namespace std;

vector<string> split(const string& sv, function<bool(char c)> fn) {
    vector<string> ret;
    int pos = 0;
    int last = 0;
    while (pos < sv.size()) {
        last = pos;
        while (pos < sv.size() and !fn(sv[pos])) pos++;
        string word = sv.substr(last, pos - last);
        if (word != "")
            ret.push_back(move(word));
        pos++;
    }
    return ret;
}

vector<KeyValue> Map(const string& filename, const string& content) {
    vector<KeyValue> ret;
    vector<string> words = split(content, [](char c) { return !(c >= 'a' and c <= 'z' or (c >= 'A' and c <= 'Z')); });
    for (const auto& word : words) {
        ret.emplace_back(word, "1");
    }
    return ret;
}

string Reduce(const vector<string>& values) {
    return to_string(values.size());
}
