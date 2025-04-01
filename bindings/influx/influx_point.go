package influx

import "fmt"

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
				case int, int32, int64:
					line += k + "=" + fmt.Sprintf("%d", value) + ","
				case float32, float64:
					line += k + "=" + fmt.Sprintf("%f", value) + ","
				case bool:
					line += k + "=" + fmt.Sprintf("%t", value) + ","
				default:
					// Handle unsupported types gracefully
					line += k + "=null,"
				}
			}
			line = line[:len(line)-1] // Remove the trailing comma
		}
		if p.Timestamp != nil {
			line += " " + fmt.Sprintf("%d", *p.Timestamp)
		}
		return line
	}
	return ""
}
