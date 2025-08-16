package utils

import "path/filepath"

func ListFiles(dir, pattern string, fullPath bool) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return nil, err
	}
	if len(files) > 0 && !fullPath {
		files = ArrayMap(files, func(t string) (bool, string) {
			return true, filepath.Base(t)
		}, true, "")
	}
	return files, nil
}
