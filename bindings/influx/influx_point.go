package influx

import (
	"strconv"
)

type InfluxPoint struct {
	Line        *string
	Measurement *string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   *int64
}

func (p *InfluxPoint) GetLine() string {
	if p.Line != nil && *p.Line != "" {
		return *p.Line
	}
	// If Line is nil, we can construct it from the other fields
	if p.Measurement != nil {
		line := *p.Measurement
		if p.Tags != nil {
			for k, v := range p.Tags {
				line += "," + k + "=" + v
			}
		}
		if p.Fields != nil {
			line += " "
			for k, v := range p.Fields {
				switch value := v.(type) {
				case string:
					line += k + "=\"" + value + "\","
				case int64:
					line += k + "=" + strconv.FormatInt(value, 10) + ","
				case int32:
					line += k + "=" + strconv.FormatInt(int64(value), 10) + ","
				case int:
					line += k + "=" + strconv.Itoa(value) + ","
				case float32:
					line += k + "=" + strconv.FormatFloat(float64(value), 'f', -1, 32) + ","
				case float64:
					line += k + "=" + strconv.FormatFloat(value, 'f', -1, 64) + ","
				case bool:
					line += k + "=" + strconv.FormatBool(value) + ","
				default:
					// Handle unsupported types gracefully
					line += k + "=null,"
				}
			}
			line = line[:len(line)-1] // Remove the trailing comma
		}
		if p.Timestamp != nil {
			line += " " + strconv.FormatInt(*p.Timestamp, 10)
		}
		return line
	}
	return ""
}
